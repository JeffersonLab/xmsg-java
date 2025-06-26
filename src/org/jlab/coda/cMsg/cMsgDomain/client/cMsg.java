/*----------------------------------------------------------------------------*
 *  Copyright (c) 2004        Southeastern Universities Research Association, *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    C. Timmer, 14-Jul-2004, Jefferson Lab                                   *
 *                                                                            *
 *     Author: Carl Timmer                                                    *
 *             timmer@jlab.org                   Jefferson Lab, MS-12B3       *
 *             Phone: (757) 269-5130             12000 Jefferson Ave.         *
 *             Fax:   (757) 269-6248             Newport News, VA 23606       *
 *                                                                            *
 *----------------------------------------------------------------------------*/

package org.jlab.coda.cMsg.cMsgDomain.client;

import org.jlab.coda.cMsg.*;
import org.jlab.coda.cMsg.common.*;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgMimeType;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgD.xMsgPayload;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;

import java.io.*;
import java.net.*;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements a cMsg client in the cMsg domain.
 *
 * @author Carl Timmer
 * @version 1.0
 */
public class cMsg extends cMsgDomainAdapter {

    /** xMsg actor being used for all pub/sub */
    private xMsg actor;

    /** The parsed form of the single UDL the client is currently connected to. */
    ParsedUDL currentParsedUDL;

    //-- FAILOVER STUFF ---------------------------------------------------------------

    /** List of parsed UDL objects - one for each failover UDL. */
    private List<ParsedUDL> failoverUdls;

    /**
     * Index into the {@link #failoverUdls} list corressponding to the UDL
     * currently being used.
     */
    private volatile byte failoverIndex;

    /**
     * If more than one viable failover UDL is given or failover to a cloud is
     * indicated, then this is true, meaning
     * if any request to the server is interrupted, that method will wait a short
     * while for the failover to complete before throwing an exception or continuing
     * on.
     */
    volatile boolean useFailovers;

    //------------------------------------------------------------------------------

    /** Socket over which to send UDP multicast and receive response packets from server. */
    private MulticastSocket udpSocket;

    /** Socket over which to send messages with UDP. */
    private DatagramSocket sendUdpSocket;

    /** Packet in which to send messages with UDP. */
    private DatagramPacket sendUdpPacket;

    /** Signal to coordinate the multicasting and waiting for responses. */
    private CountDownLatch multicastResponse;

    /** Domain server's IP addresses. */
    ArrayList<String> ipList = new ArrayList<String>(20);

    /** Domain server's broadcast/subnet addresses. */
    ArrayList<String> broadList = new ArrayList<String>(20);

    /** Domain server's host. */
    String domainServerHost;

    /** Domain server's TCP port. */
    int domainServerPort;

    /** Domain server's UDP port for udp client sends. */
    private int domainServerUdpPort;

    /** Channel for talking to domain server. */
    Socket domainOutSocket;

    /** Socket output stream associated with domainOutChannel - sends info to server. */
    DataOutputStream domainOut;

    /**
     * True if the UDLs given in the constructor have been parsed by the
     * {@link #connect} method. Need to do this only once.
     */
    private boolean udlsParsed;

    /**
     * True if the method {@link #disconnect} was called.
     */
    private volatile boolean disconnectCalled;


    /**
     * Constructor which does NOT automatically try to connect to the name server specified.
     * That's because the UDL, UDLremainder, name, description, and debug are passed in later
     * throught the setter methods. This object is constructed generically through class names
     * found by parsing the UDL at the top level.
     *
     * @throws cMsgException if local host name cannot be found
     */
    public cMsg() throws cMsgException {
        domain = "cMsg";

        failoverUdls     = Collections.synchronizedList(new ArrayList<ParsedUDL>(20));

        // store our host's name
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch (UnknownHostException e) {
            throw new cMsgException("cMsg: cannot find host name");
        }

        // create a shutdown handler class which does a disconnect
        class myShutdownHandler implements cMsgShutdownHandlerInterface {
            cMsgDomainInterface cMsgObject;

            myShutdownHandler(cMsgDomainInterface cMsgObject) {
                this.cMsgObject = cMsgObject;
            }

            public void handleShutdown() {
                try {cMsgObject.disconnect();}
                catch (cMsgException e) {}
            }
        }

        // Now make an instance of the shutdown handler
        setShutdownHandler(new myShutdownHandler(this));
    }

    /**
     * Set the UDL of the client which may be a semicolon separated list of UDLs in this domain.
     * @param UDL UDL of client
     * @throws cMsgException if UDL is null, no beginning cmsg://, no host given, unknown host
     */
    public void setUDL(String UDL) throws cMsgException {
        if (UDL == null) {
            throw new cMsgException("UDL argument is null");
        }

        this.UDL = UDL;

        // The UDL is a semicolon separated list of UDLs, separate them and
        // store them for future use in failoverUdls. Only need to do this once.
        String[] UDLstrings = UDL.split(";");

        if (debug >= cMsgConstants.debugInfo) {
            for (int i=0; i<UDLstrings.length; i++) {
                System.out.println("UDL #" + i + " = " + UDLstrings[i]);
            }
        }

        // Parse the list of UDLs and store them locally for now.
        // Throws cMsgException if there is a bad UDL in the list.
        // Do things this way so we don't ruin "failoverUdls" if there
        // is a bad UDL in the given list.
        ArrayList<ParsedUDL> failoverList = new ArrayList<ParsedUDL>(UDLstrings.length);
        for (String udl : UDLstrings) {
            failoverList.add(parseUDL(udl));
        }

        // Rewrite official list of parsed UDLs.
        synchronized (failoverUdls) {
            failoverUdls.clear();
            for (ParsedUDL p : failoverList) {
                failoverUdls.add(p);
            }
            failoverIndex = -1;

            // If we have more than one valid UDL, we can implement waiting
            // for a successful failover before aborting commands to the server
            // that were interrupted due to server failure.
            useFailovers = failoverUdls.size() > 1 ||
                    failoverUdls.get(0).failover == cMsgConstants.failoverCloud ||
                    failoverUdls.get(0).failover == cMsgConstants.failoverCloudOnly;
        }
    }


    /**
     * Get the UDL the client is currently connected to or null if no connection.
     * @return UDL the client is currently connected to or null if no connection
     */
    public String getCurrentUDL() {
        if (currentParsedUDL == null) return null;
        return currentParsedUDL.UDL;
    }


    /**
     * Get the host of the cMsg server that this client is connected to.
     * @return cMsg server's host; null if unknown
     */
    public String getServerHost() {
        if (currentParsedUDL == null) return null;
        return currentParsedUDL.nameServerHost;
    }


    /**
     * Get the TCP port of the cMsg server that this client is connected to.
     * @return cMsg server's port; 0 if unknown
     */
    public int getServerPort() {
        if (currentParsedUDL == null) return 0;
        return currentParsedUDL.nameServerTcpPort;
    }


    /**
     * Get a string of information dependent upon the argument.
     * In this domain, a cmd arg of "serverName" returns the name of the cMsg name
     * server this client is connected to in the form "IPaddress:port".
     * @return string dependent on argument's value; may be null
     */
    public String getInfo(String cmd) {
        if (cmd.equals("serverName")) {
            if (currentParsedUDL == null) return null;
            return currentParsedUDL.nameServerHost + ":" + currentParsedUDL.nameServerTcpPort;
        }
        return null;
    }


    /**
     * Method to connect to the domain server from this client.
     * This method handles multiple UDLs,
     * but passes off the real work to {@link #connectDirect}.
     *
     * @throws cMsgException if there are problems parsing the UDL or
     *                       communication problems with the server(s)
     */
    public void connect() throws cMsgException {
            // The UDL is a semicolon separated list of UDLs, separate them and
            // store them for future use in failoverUdls. Only need to do this once.
            if (!udlsParsed) {
                setUDL(UDL);
                udlsParsed = true;
            }

            cMsgException ex;

            synchronized (failoverUdls) {

                // If we're not connected, go ahead and connect.
                // If we're connected, and already using the UDL at the head of the list, return.
                // Else, disconnect from the current server and reconnect using the first UDL.
                if (connected){
                    if (currentParsedUDL != null &&
                       !currentParsedUDL.mustMulticast &&
                        currentParsedUDL.UDL.equals(failoverUdls.get(0).UDL)) {
                            if (debug >= cMsgConstants.debugInfo) {
                                System.out.println("connect: we're already connected to this UDL and we're not multicasting, so return");
                            }
                            return;
                    }
                }

                failoverIndex = -1;
                
                // Go through the UDL's until one works
                do {
                    // get parsed & stored UDL info & store UDL info locally
                    currentParsedUDL = failoverUdls.get(++failoverIndex);

                    // connect using that UDL info
                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("Trying to connect with UDL = " + currentParsedUDL.UDL);
                    }

                    try {
                        List<String> orderedIpList;

                        if (currentParsedUDL.mustMulticast) {
                            getProxyDetailsThroughMulticast();
                            // Order server IP addresses so that those on same
                            // subnet as this client are listed first
                            orderedIpList = cMsgUtilities.orderIPAddresses(ipList, broadList,
                                                                           currentParsedUDL.preferredSubnet);
                        }
                        else {
                            getProxyDetailsThroughTCP();
                            orderedIpList = new ArrayList<String>(1);
                            orderedIpList.add(currentParsedUDL.nameServerHost);
                        }

                        connectToProxy(orderedIpList);

                        return;
                    }
                    catch (cMsgException e) {
                        currentParsedUDL = null;
                        ex = e;
                    }

                } while (failoverIndex < failoverUdls.size() - 1);
            }

            throw new cMsgException("connect: all UDLs failed", ex);
        }


    /**
     * Method to multicast in order to find the domain server from this client.
     * Once the server is found and returns its host and port, a direct connection
     * can be made. Only called when connectLock is held.
     *
     * @throws cMsgException if there are problems parsing the UDL or
     *                       communication problems with the server.
     */
    protected void getProxyDetailsThroughMulticast() throws cMsgException {
        // Need a new latch for each go round - one shot deal
        multicastResponse = new CountDownLatch(1);


        //-------------------------------------------------------
        // multicast on local subnet to find cMsg server
        //-------------------------------------------------------
        DatagramPacket udpPacket;
        InetAddress multicastAddr = null;
        try {
            multicastAddr = InetAddress.getByName(cMsgNetworkConstants.cMsgMulticast);
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // create byte array for multicast
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream out = new DataOutputStream(baos);

        try {
            // send our magic ints
            out.writeInt(cMsgNetworkConstants.magicNumbers[0]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[1]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[2]);
            out.writeInt(cMsgConstants.version);
            // int describing our message type: multicast is from cMsg domain client
            out.writeInt(cMsgNetworkConstants.cMsgDomainMulticast);
            out.writeInt(currentParsedUDL.password.length());
            try {out.write(currentParsedUDL.password.getBytes("US-ASCII"));}
            catch (UnsupportedEncodingException e) { }
            out.flush();
            out.close();

            // create socket to receive at anonymous port & all interfaces
            udpSocket = new MulticastSocket();

            // Avoid local port for socket to which others may be multicasting to
            int tries = 20;
            while (udpSocket.getLocalPort() > cMsgNetworkConstants.UdpClientPortMin &&
                   udpSocket.getLocalPort() < cMsgNetworkConstants.UdpClientPortMax) {
                udpSocket = new MulticastSocket();
                if (--tries < 0) break;
            }

            udpSocket.setSoTimeout(1000);
            udpSocket.setReceiveBufferSize(1024);
            udpSocket.setTimeToLive(32); // Need to get thru routers

            // create multicast packet from the byte array
            byte[] buf = baos.toByteArray();
            udpPacket = new DatagramPacket(buf, buf.length, multicastAddr, currentParsedUDL.nameServerUdpPort);
        }
        catch (IOException e) {
            try { out.close();} catch (IOException e1) {}
            try {baos.close();} catch (IOException e1) {}
            if (udpSocket != null) udpSocket.close();
            throw new cMsgException("Cannot create multicast packet", e);
        }

        ipList.clear();
        broadList.clear();

        // create a thread which will receive any responses to our multicast
        UdpReceiver receiver = new UdpReceiver();
        receiver.start();

        // create a thread which will send our multicast
        Multicaster sender = new Multicaster(udpPacket);
        sender.start();

        // wait up to multicast timeout
        boolean response = false;
        if (currentParsedUDL.multicastTimeout > 0) {
            try {
                if (multicastResponse.await(currentParsedUDL.multicastTimeout, TimeUnit.MILLISECONDS)) {
                    response = true;
                }
            }
            catch (InterruptedException e) {
                System.out.println("INTERRUPTING WAIT FOR MULTICAST RESPONSE, (timeout specified)");
            }
        }
        // wait forever
        else {
            try { multicastResponse.await(); response = true;}
            catch (InterruptedException e) {
                System.out.println("INTERRUPTING WAIT FOR MULTICAST RESPONSE, (timeout NOT specified)");
            }
        }

        sender.interrupt();

        if (!response) {
            receiver.interrupt();
            throw new cMsgException("No response to UDP multicast received");
        }

        // Record whether this server is local or not.
        // If server client, nothing in failoverUdls so null pointer exception in get
        if (failoverIndex < failoverUdls.size()) {
            if (currentParsedUDL != null)
                currentParsedUDL.local = cMsgUtilities.isHostLocal(currentParsedUDL.nameServerHost);
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("Got a response!, multicast part finished ...");
        }
        return;
    }


    /**
     * This class gets any response to our UDP multicast.
     */
    class UdpReceiver extends Thread {

        public void run() {

            try {
                /* A slight delay here will help the main thread (calling connect)
                 * to be already waiting for a response from the server when we
                 * multicast to the server here (prompting that response). This
                 * will help insure no responses will be lost.
                 */
                Thread.sleep(200);

                byte[] buf = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buf, 1024);

                while (true) {
                    try {
                        packet.setLength(1024);
                        try {
                            udpSocket.receive(packet);
                        }
                        catch (SocketTimeoutException e) {
                            // Check to see if we've been asked to quit
                            if (Thread.interrupted()) {
                                return;
                            }
                            continue;
                        }

                        // if packet is smaller than 6 ints  ...
                        if (packet.getLength() < 24) {
                            continue;
                        }

                        //System.out.println("RECEIVED MULTICAST RESPONSE PACKET !!!");
                        // pick apart byte array received
                        int magicInt1  = cMsgUtilities.bytesToInt(buf, 0); // magic password
                        int magicInt2  = cMsgUtilities.bytesToInt(buf, 4); // magic password
                        int magicInt3  = cMsgUtilities.bytesToInt(buf, 8); // magic password

                        if ( (magicInt1 != cMsgNetworkConstants.magicNumbers[0]) ||
                             (magicInt2 != cMsgNetworkConstants.magicNumbers[1]) ||
                             (magicInt3 != cMsgNetworkConstants.magicNumbers[2]))  {
                            //System.out.println("  Bad magic numbers for multicast response packet");
                            continue;
                        }

                        currentParsedUDL.nameServerTcpPort = cMsgUtilities.bytesToInt(buf, 12); // port to do a direct connection to
                        if ((currentParsedUDL.nameServerTcpPort < 1024 || currentParsedUDL.nameServerTcpPort > 65535)) {
                            //System.out.println("  Wrong format for multicast response packet");
                            continue;
                        }

                        // udpPort is next but we'll skip over it since we don't use it

                        // # of address pairs to follow
                        int listLen = cMsgUtilities.bytesToInt(buf, 20);
                        if (listLen < 0 || listLen > 50) {
                            System.out.println("  Wrong format for multicast response packet, listLen = " + listLen);
                            continue;
                        }

                        int pos = 24;
                        String ss;
                        int stringLen;
                        ipList.clear();
                        broadList.clear();

                        for (int i=0; i < listLen; i++) {
                            try {
                                stringLen = cMsgUtilities.bytesToInt(buf, pos); pos += 4;
                                //System.out.println("     ip len = " + listLen);
                                ss = new String(buf, pos, stringLen, "US-ASCII");
                                //System.out.println("     ip = " + ss);
                                ipList.add(ss);
                                pos += stringLen;

                                stringLen = cMsgUtilities.bytesToInt(buf, pos); pos += 4;
                                //System.out.println("     broad len = " + listLen);
                                ss = new String(buf, pos, stringLen, "US-ASCII");
                                //System.out.println("     broad = " + ss);
                                broadList.add(ss);
                                pos += stringLen;
                            }
                            catch (UnsupportedEncodingException e) {/*never happen */}
                        }
                        break;
                    }
                    catch (Exception e) {
                    }
                }

                multicastResponse.countDown();
            }
            catch (InterruptedException e) {
                // time to quit
                System.out.println("Interrupted receiver");
            }
        }
    }


    /**
     * This class defines a thread to multicast a UDP packet to the
     * cMsg name server every second.
     */
    class Multicaster extends Thread {

        DatagramPacket packet;

        Multicaster(DatagramPacket udpPacket) {
            packet = udpPacket;
        }

        public void run() {

            try {
                /* A slight delay here will help the main thread (calling connect)
                * to be already waiting for a response from the server when we
                * multicast to the server here (prompting that response). This
                * will help insure no responses will be lost.
                */
                Thread.sleep(100);

                while (true) {

                    try {
                        if (debug >= cMsgConstants.debugInfo) {
                            System.out.println("Send multicast packet to cMsg server");
                        }
                        // send a packet over each network interface
                        Enumeration<NetworkInterface> enumer = NetworkInterface.getNetworkInterfaces();
                        while (enumer.hasMoreElements()) {
                            NetworkInterface ni = enumer.nextElement();
                            if (ni.isUp()) {
                                udpSocket.setNetworkInterface(ni);
                                udpSocket.send(packet);
                            }
                        }
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }

                    Thread.sleep(1000);
                }
            }
            catch (InterruptedException e) {
                // time to quit
                System.out.println("Interrupted sender");
            }
        }
    }


    /**
     * Method to make the actual connection to the name & domain servers from this client.
     * Only called by method protected with connectLock (write lock), so no mutex
     * protection needed. Only called if not already connected.
     *
     * @param addrs list of server IP addresses in order of those with same subnet
     *              as this client first, others last
     * @throws cMsgException if there are problems parsing the UDL or
     *                       communication problems with the server
     */
    private void connectToProxy(List<String> addrs) throws cMsgException {

        // Connect to cMsg server proxy
        xMsg currActor = null;

        for (String ip : addrs) {
                currActor = new xMsg("actor",
                        new xMsgProxyAddress(ip, domainServerPort),
                        new xMsgRegAddress(),
                        xMsgConstants.DEFAULT_POOL_SIZE);
                 try {
                    currActor.cacheConnection();
                    this.actor = currActor;
                    this.connected = true;
                    this.domainServerHost = ip; 
                    currentParsedUDL.nameServerHost = ip;
                    break;
                } catch (xMsgException e) {
                    currActor.destroy();
                    System.err.println("connectToProxy: connection not possible on interface: " + ip + " at port: " + domainServerPort);
                }
            }

            if (this.actor == null) {
                throw new cMsgException("connectToProxy: cannot connect to proxy server on any interface");
            }
        }


    /**
     * This method does nothing.
     * @param timeout ignored
     */
    public void flush(int timeout) {}


    /**
     * Method to close the connection to the domain server. This method results in this object
     * becoming functionally useless.
     */
    public void disconnect() {
        this.actor.destroy();
        connected = false;
        disconnectCalled = true;
    }


    /**
     * This method gets the host and port of the domain server from the name server.
     * It also gets information about the subdomain handler object.
     * Note to those who would make changes in the protocol, keep the first three
     * ints the same. That way the server can reliably check for mismatched versions.
     *
     * @param socket socket to server
     * @throws IOException if there are communication problems with the name server
     * @throws cMsgException if the name server's domain does not match the UDL's domain;
     *                       the client cannot be registered; the domain server cannot
     *                       open a listening socket or find a port to listen on; or
     *                       the name server cannot establish a connection to the client
     */
    void getProxyDetailsThroughTCP() throws cMsgException {
        Socket socket = null;
        DataInputStream in = null;
        DataOutputStream out = null;

        try {
            // Connect to name server
            socket = new Socket(currentParsedUDL.nameServerHost, currentParsedUDL.nameServerTcpPort);
            socket.setTcpNoDelay(true);

            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            // Write handshake and request
            out.writeInt(cMsgNetworkConstants.magicNumbers[0]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[1]);
            out.writeInt(cMsgNetworkConstants.magicNumbers[2]);
            out.writeInt(cMsgConstants.version);
            out.writeInt(cMsgNetworkConstants.cMsgDomainTCP);
            out.writeInt(currentParsedUDL.password.length());

            try {
                out.write(currentParsedUDL.password.getBytes("US-ASCII"));
            } catch (UnsupportedEncodingException e) {
                throw new cMsgException("getProxyDetailsThroughTCP: Unsupported encoding when writing password: " + e.getMessage(), e);
            }

            out.flush();

            // Read acknowledgment from server
            int error;
            try {
                error = in.readInt();
            } catch (IOException e) {
                throw new cMsgException("getProxyDetailsThroughTCP: did not get any response from server: " + e.getMessage(), e);
            }

            // If server reported an error, read the error message
            if (error != cMsgConstants.ok) {
                int len;
                try {
                    len = in.readInt();
                    byte[] buf = new byte[len + 100];
                    in.readFully(buf, 0, len);
                    String err = new String(buf, 0, len, "US-ASCII");

                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("getProxyDetailsThroughTCP: error from server: " + err);
                    }

                    throw new cMsgException("Error from server: " + err);
                } catch (IOException e) {
                    throw new cMsgException("getProxyDetailsThroughTCP: Failed while reading error message from server: " + e.getMessage(), e);
                }
            }

            // Read domain server port (host info may also follow if needed)
            try {
                domainServerPort = in.readInt();
            } catch (IOException e) {
                throw new cMsgException("getProxyDetailsThroughTCP: Failed to read domain server port: " + e.getMessage(), e);
            }

            if (debug >= cMsgConstants.debugInfo) {
                System.out.println("        << CL: domain server port = " + domainServerPort);
            }

        } catch (IOException e) {
            throw new cMsgException("getProxyDetailsThroughTCP: IO error while connecting or communicating: " + e.getMessage(), e);
        } finally {
            try { if (in != null) in.close(); } catch (IOException e) { /* Ignore close exception */ }
            try { if (out != null) out.close(); } catch (IOException e) { /* Ignore close exception */ }
            try { if (socket != null) socket.close(); } catch (IOException e) { /* Ignore close exception */ }
        }
    }


    /**
     * <p>Method to parse the Universal Domain Locator (UDL) into its various components.
     * The general cMsg domain UDL is of the form:</p>
     *
     *   <p><b>cMsg:cMsg://&lt;host&gt;:&lt;port&gt;/&lt;subdomainType&gt;/&lt;subdomain remainder&gt;?tag=value&amp;tag2=value2 ...</b></p>
     *
     * <ul>
     * <li><p>port is not necessary to specify but is the name server's TCP port if connecting directly
     *     or the server's UDP port if multicasting. If not specified, defaults are
     *     {@link cMsgNetworkConstants#nameServerTcpPort} if connecting directly, else
     *     {@link cMsgNetworkConstants#nameServerUdpPort} if multicasting</p>
     * <li><p>host can be "localhost" and may also be in dotted form (129.57.35.21), but may not contain a colon.
     *     It can also be "multicast"</p>
     * <li><p>if domainType is cMsg, subdomainType is automatically set to cMsg if not given.
     *     if subdomainType is not cMsg, it is required</p>
     * <li><p>the domain name is case insensitive as is the subdomainType</p>
     * <li><p>remainder is passed on to the subdomain plug-in</p>
     * <li><p>client's password is in tag=value part of UDL as cmsgpassword=&lt;password&gt;</p>
     * <li><p>domain server port is in tag=value part of UDL as domainPort=&lt;port&gt;</p>
     * <li><p>multicast timeout is in tag=value part of UDL as multicastTO=&lt;time out in seconds&gt;</p>
     * <li><p>subnet is in tag=value part of UDL as subnet=&lt;preferred subnet in dot-decimal&gt;</p>
     * <li><p>failover is in tag=value part of UDL as failover=&lt;cloud, cloudonly, or any&gt;</p>
     * <li><p>cloud is in tag=value part of UDL as failover=&lt;local or any&gt;</p>
     * <li><p>the tag=value part of UDL parsed here as regime=low or regime=high means:</p>
     *   <ul>
     *   <li><p>low message/data throughput client if regime=low, meaning many clients are serviced
     *       by a single server thread and all msgs retain time order</p>
     *   <li><p>high message/data throughput client if regime=high, meaning each client is serviced
     *       by multiple threads to maximize throughput. Msgs are NOT guaranteed to be handled in
     *       time order</p>
     *   <li><p>if regime is not specified (default), it is assumed to be medium, where a single thread is
     *       dedicated to a single client and msgs are guaranteed to be handled in time order</p>
     *   </ul>
     * </ul>
     *
     * The first "cMsg:" is optional. IN the case of the "cMsg" subdomain, its presence in the udl
     * is also optional.
     * The cMsg subdomain interprets the subdoman remainder as a namespace in which  messages live
     * with no crossing of messages into other namespaces.
     *
     * @param udl UDL to parse
     * @return an object with all the parsed UDL information in it
     * @throws cMsgException if UDL is null, no beginning cmsg://, no host given, unknown host
     */
    ParsedUDL parseUDL(String udl) throws cMsgException {

        if (udl == null) {
            throw new cMsgException("invalid UDL");
        }

        // strip off the cMsg:cMsg:// to begin with
        String udlLowerCase = udl.toLowerCase();
        int index = udlLowerCase.indexOf("cmsg://");
        if (index < 0) {
            throw new cMsgException("invalid UDL");
        }
        String udlRemainder = udl.substring(index+7);

        Pattern pattern = Pattern.compile("([^:/]+):?(\\d+)?/?(\\w+)?/?(.*)");
        Matcher matcher = pattern.matcher(udlRemainder);

        String udlHost, udlPort, udlSubdomain, udlSubRemainder;

        if (matcher.find()) {
            // host
            udlHost = matcher.group(1);
            // port
            udlPort = matcher.group(2);
            // subdomain
            udlSubdomain = matcher.group(3);
            // remainder
            udlSubRemainder = matcher.group(4);

            if (debug >= cMsgConstants.debugInfo) {
                System.out.println("\nparseUDL: " +
                                   "\n  host      = " + udlHost +
                                   "\n  port      = " + udlPort +
                                   "\n  subdomain = " + udlSubdomain +
                                   "\n  remainder = " + udlSubRemainder);
            }
        }
        else {
            throw new cMsgException("invalid UDL");
        }

        // need at least host
        if (udlHost == null) {
            throw new cMsgException("invalid UDL");
        }

        // if subdomain not specified, use cMsg subdomain
        if (udlSubdomain == null) {
            udlSubdomain = "cMsg";
        }
        else {
            // All recognized subdomains
            String[] allowedSubdomains = {"cMsg"};

            // Make sure the sub domain is recognized.
            // Because the cMsg subdomain is the only one in which a "/" is contained
            // in the remainder, and because the presence of the "cMsg" subdomain identifier
            // is optional, what will happen when it's parsed is that the namespace will be
            // interpreted as the subdomain if "cMsg" domain identifier is not there.
            // Thus we must take care of this case. If we don't recognize the subdomain,
            // assume it's the namespace of the cMsg subdomain.
            boolean foundSubD = false;
            for (String subDom: allowedSubdomains) {
                if (subDom.equalsIgnoreCase(udlSubdomain)) {
                    foundSubD = true;
                    break;
                }
            }

            if (!foundSubD) {
                if (udlSubRemainder == null) {
                    udlSubRemainder = udlSubdomain;
                }
                else {
                    udlSubRemainder = udlSubdomain + "/" + udlSubRemainder;
                }
                udlSubdomain = "cMsg";
            }
        }

        boolean isLocal = false;
        boolean mustMulticast = false;
        if (udlHost.equalsIgnoreCase("multicast") ||
            udlHost.equals(cMsgNetworkConstants.cMsgMulticast)) {
            mustMulticast = true;
            // "isLocal" must be determined after connection, set it false for now
            //System.out.println("set mustMulticast to true (locally in parse method)");
        }
        // if the host is "localhost", find the actual, fully qualified  host name
        else if (udlHost.equalsIgnoreCase("localhost")) {
            try {udlHost = InetAddress.getLocalHost().getCanonicalHostName();}
            catch (UnknownHostException e) {
                throw new cMsgException("cannot find localhost", e);
            }
            isLocal = true;

            if (debug >= cMsgConstants.debugWarn) {
               System.out.println("parseUDL: name server given as \"localhost\", substituting " +
                                  udlHost);
            }
        }
        else {
            try {InetAddress.getByName(udlHost);}
            catch (UnknownHostException e) {
                throw new cMsgException("unknown host", e);
            }
            isLocal = cMsgUtilities.isHostLocal(udlHost);
        }

        // get name server port or guess if it's not given
        int tcpPort, udpPort;
        if (udlPort != null && udlPort.length() > 0) {
            try {
                int udlPortInt = Integer.parseInt(udlPort);
                if (udlPortInt < 1024 || udlPortInt > 65535) {
                    throw new cMsgException("parseUDL: illegal port number");
                }

                if (mustMulticast) {
                    udpPort = udlPortInt;
                    tcpPort = cMsgNetworkConstants.nameServerTcpPort;
                }
                else {
                    udpPort = cMsgNetworkConstants.nameServerUdpPort;
                    tcpPort = udlPortInt;
                }
            }
            // should never happen
            catch (NumberFormatException e) {
                udpPort = cMsgNetworkConstants.nameServerUdpPort;
                tcpPort = cMsgNetworkConstants.nameServerTcpPort;
            }
        }
        else {
            udpPort = cMsgNetworkConstants.nameServerUdpPort;
            tcpPort = cMsgNetworkConstants.nameServerTcpPort;
            if (debug >= cMsgConstants.debugWarn) {
                System.out.println("parseUDL: guessing name server TCP port = " + tcpPort +
                 ", UDP port = " + udpPort);
            }
        }

        // any remaining UDL is ...
        if (udlSubRemainder == null) {
            udlSubRemainder = "";
        }

        // don't allow multiple, identical tags
        int counter = 0;

        // find cmsgpassword parameter if it exists
        String pswd = "";
        pattern = Pattern.compile("[\\?&]cmsgpassword=([^&]+)", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(udlSubRemainder);
        while (matcher.find()) {
            pswd = matcher.group(1);
            counter++;
            //System.out.println("  cmsg password = " + pswd);
        }

        if (counter > 1) {
            throw new cMsgException("parseUDL: only 1 password allowed");
        }

        // look for multicastTO=value
        counter=0;
        int timeout=0;
        pattern = Pattern.compile("[\\?&]multicastTO=([^&]+)", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(udlSubRemainder);
        while (matcher.find()) {
            //System.out.println("multicast timeout = " + matcher.group(1));
            try {
                timeout = 1000 * Integer.parseInt(matcher.group(1));
                if (timeout < 0) {
                    throw new cMsgException("parseUDL: multicast timeout must be integer >= 0");
                }
                counter++;
            }
            catch (NumberFormatException e) {
                throw new cMsgException("parseUDL: multicast timeout must be integer >= 0");
            }
        }

        if (counter > 1) {
            throw new cMsgException("parseUDL: only 1 multicast timeout allowed");
        }

        // look for domainPort=value
        counter=0;
        int domainPort=0;
        pattern = Pattern.compile("[\\?&]domainPort=([^&]+)", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(udlSubRemainder);
        while (matcher.find()) {
            //System.out.println("domain server port = " + matcher.group(1));
            try {
                domainPort = Integer.parseInt(matcher.group(1));
                if (domainPort < 1024 || domainPort > 65535) {
                    throw new cMsgException("parseUDL: domain server illegal port number");
                }
                counter++;
            }
            catch (NumberFormatException e) {
                throw new cMsgException("parseUDL: domain server port must be integer > 1023 and < 65536");
            }
        }

        if (counter > 1) {
            throw new cMsgException("parseUDL: only 1 domain server port allowed");
        }

        // look for subnet=dot-decimal IP address
        counter=0;
        String preferredSubnet=null;
        pattern = Pattern.compile("[\\?&]subnet=((?:[0-9]{1,3}\\.){3}[0-9]{1,3})", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(udlSubRemainder);
        while (matcher.find()) {
            //System.out.println("preferred subnet = " + matcher.group(1));
            preferredSubnet = matcher.group(1);
            counter++;
        }

        if (counter > 1) {
            throw new cMsgException("parseUDL: only 1 preferred subnet allowed");
        }

        // look for regime=low, medium, or high
        counter=0;
        int regime = cMsgConstants.regimeMedium;
        pattern = Pattern.compile("[\\?&]regime=([^&]+)", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(udlSubRemainder);
        while (matcher.find()) {
            //System.out.println("regime = " + matcher.group(1));
            if (matcher.group(1).equalsIgnoreCase("low")) {
                regime = cMsgConstants.regimeLow;
            }
            else if (matcher.group(1).equalsIgnoreCase("high")) {
                regime = cMsgConstants.regimeHigh;
            }
            else if (matcher.group(1).equalsIgnoreCase("medium")) {
                regime = cMsgConstants.regimeMedium;
            }
            else {
                throw new cMsgException("parseUDL: regime must be low, medium or high");
            }
            counter++;
        }

        if (counter > 1) {
            throw new cMsgException("parseUDL: only 1 regime value allowed");
        }

        // look for failover=cloud, cloudonly, any
        counter=0;
        int failover = cMsgConstants.failoverAny;
        pattern = Pattern.compile("[\\?&]failover=([^&]+)", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(udlSubRemainder);
        while (matcher.find()) {
            //System.out.println("failover = " + matcher.group(1));
            if (matcher.group(1).equalsIgnoreCase("cloud")) {
                failover = cMsgConstants.failoverCloud;
            }
            else if (matcher.group(1).equalsIgnoreCase("cloudonly")) {
                failover = cMsgConstants.failoverCloudOnly;
            }
            // "any" / default
            else {
                failover = cMsgConstants.failoverAny;
            }
            counter++;
        }

        if (counter > 1) {
            throw new cMsgException("parseUDL: only 1 failover value allowed");
        }

        // look for cloud=local, localnow, any
        counter=0;
        int cloud = cMsgConstants.cloudAny;
        pattern = Pattern.compile("[\\?&]cloud=([^&]+)", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(udlSubRemainder);
        while (matcher.find()) {
            //System.out.println("cloud = " + matcher.group(1));
            if (matcher.group(1).equalsIgnoreCase("local")) {
                cloud = cMsgConstants.cloudLocal;
            }
            else {
                // "any" / default to connecting to cloud located anywhere
                cloud = cMsgConstants.cloudAny;
            }
            counter++;
        }

        if (counter > 1) {
            throw new cMsgException("parseUDL: only 1 cloud value allowed");
        }

        // store results in a class
        return new ParsedUDL(udl, udlRemainder, udlSubdomain, udlSubRemainder,
                             pswd, udlHost, preferredSubnet, tcpPort, domainPort,
                             udpPort, timeout, regime, failover, cloud,
                             mustMulticast, isLocal);
    }


    /** This class simply holds information obtained from parsing a UDL or
     *  information about an available cMsg server in a server cloud. */
    class ParsedUDL {
        // used only for cloud server spec
        String  serverName;

        // all used in parsed UDL, some in cloud server spec
        /**
         * The Uniform Domain Locator which tells the location of a domain. It is of the
         * form cMsg:&lt;domainType&gt;://&lt;domain dependent remainder&gt;
         */
        String  UDL;
        /** String containing the remainder part of the UDL. */
        String  UDLremainder;
        /** Subdomain being used. */
        String  subdomain;
        /** Subdomain remainder part of the UDL. */
        String  subRemainder;
        /** Optional password included in UDL for connection to server requiring one. */
        String  password;
        /** Optional preferred subnet. */
        String  preferredSubnet;

        /** Name server's host. */
        String  nameServerHost;
        /** Name server's TCP port. */
        int     nameServerTcpPort;
        /** Name server's domain server TCP port. */
        int     domainServerTcpPort;
        /** Name server's UDP port. */
        int     nameServerUdpPort;

        /** Timeout in milliseconds to wait for server to respond to multicasts. */
        int     multicastTimeout;
        /**
         * What throughput do we expect from this client? Values may be one of:
         * {@link cMsgConstants#regimeHigh}, {@link cMsgConstants#regimeMedium},
         * {@link cMsgConstants#regimeLow}.
         */
        int     regime;
        /**
         * Manner in which to failover. Set to {@link cMsgConstants#failoverAny} if
         * failover of client can go to any of the UDLs given in {@link cMsg#connect} (default).
         * Set to {@link cMsgConstants#failoverCloud} if failover of client will go to
         * servers in the cloud first, and if none are available, then go to any of
         * the UDLs given in connect. Set to {@link cMsgConstants#failoverCloudOnly}
         * if failover of client will only go to servers in the cloud.
         */
        int     failover;
        /**
         * Manner in which to failover to a cloud. Set to {@link cMsgConstants#cloudAny}
         * if failover of client to a server in the cloud will go to any of the cloud servers
         * (default). Set to {@link cMsgConstants#cloudLocal} if failover of client to a server
         * in the cloud will go to a local cloud server first before others are considered.
         */
        int     cloud;
        /**
         * True if the host given by the UDL is \"multicast\"
         * or the string {@link cMsgNetworkConstants#cMsgMulticast}.
         * In this case we must multicast to find the server, else false.
         */
        boolean mustMulticast;
        /** Is this client connected to a local server? */
        boolean local;



        /**
         * Constructor for storing info about cloud server and used in
         *  failing over to cloud server.
         *
         * @param name name of cloud server (host:port)
         * @param pswrd password to connect to cloud server
         * @param host host cloud server is running on
         * @param tcpPort TCP port cloud server is listening on
         * @param udpPort UDP multicasting port cloud server is listening on
         * @param isLocal is cloud server running on localhost
         */
        ParsedUDL(String name, String pswrd, String host,
                  int tcpPort, int udpPort,  boolean isLocal) {
            serverName          = name;
            password            = pswrd == null ? "" : pswrd;
            nameServerHost      = host;
            nameServerTcpPort   = tcpPort;
            nameServerUdpPort   = udpPort;
            local               = isLocal;
        }

        /**
         * Constructor used in parsing UDL.
         * @param s1 UDL
         * @param s2 UDLremainder
         * @param s3 subdomain
         * @param s4 subRemainder
         * @param s5 password
         * @param s6 nameServerHost
         * @param s7 preferredSubnet
         *
         * @param i1 nameServerTcpPort
         * @param i2 domainServerTcpPort
         * @param i3 nameServerUdpPort
         * @param i4  multicastTimeout
         * @param i5 regime
         * @param i6 failover
         * @param i7 cloud
         *
         * @param b1 mustMulticast
         * @param b2 local
         */
        ParsedUDL(String s1, String s2, String s3, String s4, String s5, String s6, String s7,
                  int i1, int i2, int i3, int i4, int i5, int i6, int i7, boolean b1, boolean b2) {
            UDL                 = s1;
            UDLremainder        = s2;
            subdomain           = s3;
            subRemainder        = s4;
            password            = s5;
            nameServerHost      = s6;
            preferredSubnet     = s7;

            nameServerTcpPort   = i1;
            domainServerTcpPort = i2;
            nameServerUdpPort   = i3;
            multicastTimeout    = i4;
            regime              = i5;
            failover            = i6;
            cloud               = i7;
            mustMulticast       = b1;
            local               = b2;
        }

        /** Take all of this object's parameters and copy to this client's members. */
        public String toString() {
            StringBuilder sb = new StringBuilder(1024);

            sb.append("Copy from stored parsed UDL to local :");
            sb.append("  UDL                 = "); sb.append(UDL);
            sb.append("  UDLremainder        = "); sb.append(UDLremainder);
            sb.append("  subdomain           = "); sb.append(subdomain);
            sb.append("  subRemainder        = "); sb.append(subRemainder);
            sb.append("  password            = "); sb.append(password);
            sb.append("  preferredSubnet     = "); sb.append(preferredSubnet);
            sb.append("  nameServerHost      = "); sb.append(nameServerHost);
            sb.append("  nameServerTcpPort   = "); sb.append(nameServerTcpPort);
            sb.append("  domainServerTcpPort = "); sb.append(domainServerTcpPort);
            sb.append("  nameServerUdpPort   = "); sb.append(nameServerUdpPort);
            sb.append("  multicastTimeout    = "); sb.append(multicastTimeout);
            sb.append("  mustMulticast       = "); sb.append(mustMulticast);
            sb.append("  isLocal             = "); sb.append(local);

            if (regime == cMsgConstants.regimeHigh)
                sb.append("  regime              = high");
            else if (regime == cMsgConstants.regimeLow)
                sb.append("  regime              = low");
            else
                sb.append("  regime              = medium");

            if (failover == cMsgConstants.failoverAny)
                sb.append("  failover            = any");
            else if (failover == cMsgConstants.failoverCloud)
                sb.append("  failover            = cloud");
            else
                sb.append("  failover            = cloud only");

            if (cloud == cMsgConstants.cloudAny)
                sb.append("  cloud               = any");
            else if (cloud == cMsgConstants.cloudLocal)
                sb.append("  cloud               = local");
            else
                sb.append("  cloud               = local now");

            return sb.toString();
        }

    }

}