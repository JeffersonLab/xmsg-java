/*----------------------------------------------------------------------------*
 *  Copyright (c) 2004        Southeastern Universities Research Association, *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    C. Timmer, 7-Jul-2004, Jefferson Lab                                    *
 *                                                                            *
 *     Author: Carl Timmer                                                    *
 *             timmer@jlab.org                   Jefferson Lab, MS-6B         *
 *             Phone: (757) 269-5130             12000 Jefferson Ave.         *
 *             Fax:   (757) 269-5800             Newport News, VA 23606       *
 *                                                                            *
 *----------------------------------------------------------------------------*/

package org.jlab.coda.cMsg.cMsgDomain.server;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.lang.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;

import org.jlab.coda.cMsg.*;
import org.jlab.coda.cMsg.remoteExec.IExecutorThread;
import org.jlab.coda.cMsg.common.cMsgSubdomainInterface;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.sys.xMsgProxy;

/**
 * This class implements a cMsg name server in the cMsg domain.
 *
 * @author Carl Timmer
 * @version 1.0
 */
public class cMsgNameServer extends Thread implements IExecutorThread {

    /** This xMsg proxy */
    xMsgProxy proxy;
    
    /** Contains xMsgProxy port */
    byte [] successResponse;

    /** Contains error msg */
    byte [] errorResponse;

    /** This server's name. */
    String serverName;

    /** Host this server is running on. */
    private String host;

    /** This server's TCP listening port number. */
    private int port;

    /** This server's UDP listening port number for receiving multicasts. */
    private int multicastPort;

    /** The maximum value for the cMsgDomainServer's listening port number. */
    static int domainPortMax = 65535;

    /** The value of the TCP listening port for establishing permanent client connections. */
    static int domainServerPort;

    /** Server channel (contains socket). */
    private ServerSocketChannel serverChannel;

    /** UDP socket on which to read multicast packets sent from cMsg clients. */
    private MulticastSocket multicastSocket;

    /** Thread which receives client multicasts. */
    private cMsgMulticastListeningThread multicastThread;

    /**
     * There are 3 threads which must be running before client connections are allowed.
     * Use this object to signal the point at which both of these threads have been
     * successfully started (during {@link #startServer()}) .
     */
    CountDownLatch preConnectionThreadsStartedSignal = new CountDownLatch(3);

    /**
     * There are 2 threads which must be running before client connections are allowed.
     * Use this object to signal the point at which both of these threads have been
     * successfully started (during {@link #startServer()}) .
     */
    CountDownLatch postConnectionThreadsStartedSignal = new CountDownLatch(2);

    /** Level of debug output for this class. */
    private int debug;

    /** Tell the server to kill spawned threads. */
    private boolean killAllThreads;

    /**
     * Sets boolean to kill this and all spawned threads.
     * @param b setting to true will kill this and all spawned threads
     */
    private void setKillAllThreads(boolean b) {killAllThreads = b;}

    /**
     * Gets boolean value specifying whether to kill this and all spawned threads.
     * @return boolean value specifying whether to kill this and all spawned threads
     */
    private boolean getKillAllThreads() {return killAllThreads;}

    /**
     * List of all ClientHandler objects. This list is used to
     * end these threads nicely during a shutdown.
     */
    private ArrayList<ClientHandler> handlerThreads;
    
    /**
     * Password that clients need to match before being allowed to connect.
     * This is subdomain independent and applies to the server as a whole.
     * If this is null and the client supplies a password anyway, that is also
     * forbidden. In this way, this password acts as a unique name for this
     * cMsg server.
     */
    String clientPassword;

    /**
     * Use this to signal that this server's listening threads have been started
     * so bridges may be created and clients may connect.
     */
    CountDownLatch listeningThreadsStartedSignal = new CountDownLatch(2);

    //--------------------------------------------------------
    //--------------------------------------------------------

    /**
     * Get this server's name (host:port).
     * @return server's name
     */
    public String getServerName() {
         return serverName;
     }


    /**
     * Get the host this server is running on.
     * @return server's name
     */
    public String getHost() {
         return host;
     }


    /**
     * Get the name server's listening port.
     * @return name server's listening port
     */
    public int getPort() {
        return port;
    }


    /**
     * Get the domain server's listening port.
     * @return domain server's listening port
     */
    public int getDomainPort() {
        return domainServerPort;
    }


    /**
     * Get name server's multicast listening port.
     * @return name server's multicast listening port
     */
    public int getMulticastPort() {
        return multicastPort;
    }


    /**
     * Constructor which reads environmental variables and opens listening sockets.
     *
     * @param port TCP listening port for communication from clients
     * @param domainPort  listening port for receiving 2 permanent connections from each client
     * @param udpPort UDP listening port for receiving multicasts from clients
     * @param standAlone   if true no other cMsg servers are allowed to attached to this one and form a cloud
     * @param monitoringOff if true clients are NOT sent monitoring data
     * @param clientPassword password client needs to provide to connect to this server
     * @param cloudPassword  password server needs to provide to connect to this server to become part of a cloud
     * @param serverToJoin server whose cloud this server is to be joined to
     * @param debug desired level of debug output
     * @param clientsMax max number of clients per cMsgDomainServerSelect object for regime = low
     */
    public cMsgNameServer(int port, int domainPort, int udpPort,
                          boolean standAlone, boolean monitoringOff,
                          String clientPassword, String cloudPassword, String serverToJoin,
                          int debug, int clientsMax) {  
        handlerThreads = new ArrayList<ClientHandler>(10);

        this.debug          = debug;
        this.clientPassword = clientPassword;
        // read env variable for domain server port number
        if (domainPort < 1) {
            try {
                String env = System.getenv("CMSG_DOMAIN_PORT");
                if (env != null) {
                    domainPort = Integer.parseInt(env);
                }
            }
            catch (NumberFormatException ex) {
                System.out.println("Bad port number specified in CMSG_DOMAIN_PORT env variable");
                System.exit(-1);
            }
        }

        if (domainPort < 1) {
            domainPort = cMsgNetworkConstants.domainServerPort;
        }
        domainServerPort = domainPort;

        // port #'s < 1024 are reserved
        if (domainServerPort < 1024) {
            System.out.println("Domain server port number must be > 1023");
            System.exit(-1);
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println(">> NS: Domain server port = " + domainServerPort);
        }

        // read env variable for starting (desired) port number
        if (port < 1) {
            try {
                String env = System.getenv("CMSG_PORT");
                if (env != null) {
                    port = Integer.parseInt(env);
                }
            }
            catch (NumberFormatException ex) {
                System.out.println("Bad port number specified in CMSG_PORT env variable");
                System.exit(-1);
            }
        }

        if (port < 1) {
            port = cMsgNetworkConstants.nameServerTcpPort;
        }

        // port #'s < 1024 are reserved
        if (port < 1024) {
            System.out.println("Port number must be > 1023");
            System.exit(-1);
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println(">> NS: Name server TCP port = " + port);
        }

        // read env variable for starting (desired) UDP port number
        if (udpPort < 1) {
            try {
                String env = System.getenv("CMSG_MULTICAST_PORT");
                if (env != null) {
                    udpPort = Integer.parseInt(env);
                }
            }
            catch (NumberFormatException ex) {
                System.out.println("Bad port number specified in CMSG_MULTICAST_PORT env variable");
                System.exit(-1);
            }
        }

        if (udpPort < 1) {
            udpPort = cMsgNetworkConstants.nameServerUdpPort;
        }

        // port #'s < 1024 are reserved
        if (udpPort < 1024) {
            System.out.println("\nPort number must be > 1023");
            System.exit(-1);
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println(">> NS: Name server UDP port = " + udpPort);
        }

        // At this point, bind to the TCP listening port. If that isn't possible, throw
        // an exception.
        try {
            serverChannel = ServerSocketChannel.open();
        }
        catch (IOException ex) {
            System.out.println("Exiting Server: cannot open a listening socket");
            System.exit(-1);
        }

        ServerSocket listeningSocket = serverChannel.socket();

        try {
            listeningSocket.setReuseAddress(true);
            // prefer low latency, short connection times, and high bandwidth in that order
            listeningSocket.setPerformancePreferences(1,2,0);
            //System.out.println("Listening socket binding to port " + port);
            listeningSocket.bind(new InetSocketAddress("0.0.0.0", port));
        }
        catch (IOException ex) {
            System.out.println("TCP port number " + port + " in use.");
            System.exit(-1);
        }

        this.port = port;

        // Create a UDP socket for accepting multicasts from cMsg clients
        try {
            // create multicast socket to receive at all interfaces

            // Lots of bugs with multicast sockets:
            //   http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4701650
            //   http://wiki.jboss.org/wiki/CrossTalking
            // Because there are so many problems with underlying operating systems,
            // do NOT bind to the multicast address, only bind to the port.
            // Practically that means other multicasts, broadcasts or unicasts to
            // that port will get through. We'll have to implement our own filter.
            multicastSocket = new MulticastSocket(udpPort);
            multicastSocket.setReceiveBufferSize(65535);
            multicastSocket.setReuseAddress(true);
            multicastSocket.setTimeToLive(32);
//            multicastSocket.bind(new InetSocketAddress(udpPort));
            // If using standalone laptop, joinGroup throws an exception:
            // java.net.SocketException: No such device. This catch allows
            // usage with messed up / nonexistant network.
            // Be sure to join the multicast addr group on each interface
            // (something not mentioned in any javadocs or books!).
            SocketAddress sa;
            Enumeration<NetworkInterface> enumer = NetworkInterface.getNetworkInterfaces();
            System.out.println("Multicast group: " + cMsgNetworkConstants.cMsgMulticast);
            while (enumer.hasMoreElements()) {
                try {
                    NetworkInterface ni = enumer.nextElement();
                    System.out.println("Joining multicast group on interface: " + ni.getName() + " (" + ni.getDisplayName() + ")");
                    sa = new InetSocketAddress(InetAddress.getByName(cMsgNetworkConstants.cMsgMulticast), udpPort);
                    multicastSocket.joinGroup(sa, ni);
                }
                catch (IOException e) {/* cannot join multicast group cause network messed up */}
            }
        }
        catch (IOException e) {
            System.out.println("UDP port number " + udpPort + " in use.");
            System.exit(-1);
        }
        multicastPort = udpPort;

        // record our own name
        try {
            serverName = InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch (UnknownHostException e) {
        }
        serverName = serverName + ":" + port;

        // Host this is running on
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch (UnknownHostException ex) {}
    }


    /** Method to print out correct program command line usage. */
    private static void usage() {
        System.out.println("\nUsage: java [-Dport=<tcp listening port>]\n"+
                             "            [-DdomainPort=<domain server listening port>]\n" +
                             "            [-Dudp=<udp listening port>]\n" +
                             "            [-DsubdomainName=<className>]\n" +
                             "            [-Dserver=<hostname:serverport>]\n" +
                             "            [-Ddebug=<level>]\n" +
                             "            [-Dstandalone]\n" +
                             "            [-DmonitorOff]\n" +
                             "            [-Dpassword=<password>]\n" +
                             "            [-Dcloudpassword=<password>]\n" +
                             "            [-DlowRegimeSize=<size>]  cMsgNameServer\n");
        System.out.println("       port           is the TCP port this server listens on");
        System.out.println("       domainPort     is the TCP port this server listens on for connection to domain server");
        System.out.println("       udp            is the UDP port this server listens on for multicasts");
        System.out.println("       subdomainName  is the name of a subdomain and className is the");
        System.out.println("                      name of the java class used to implement the subdomain");
        System.out.println("       server         punctuation (not colon) or white space separated list of servers\n" +
                           "                      in host:port format to connect to in order to gain entry to cloud\n" +
                           "                      of servers. First successful connection used. If no connections made,\n" +
                           "                      no error indicated.");
        System.out.println("       debug          debug output level has acceptable values of:");
        System.out.println("                          info   for full output");
        System.out.println("                          warn   for severity of warning or greater");
        System.out.println("                          error  for severity of error or greater");
        System.out.println("                          severe for severity of \"cannot go on\"");
        System.out.println("                          none   for no debug output (default)");
        System.out.println("       standalone     means no other servers may connect or vice versa,");
        System.out.println("                      is incompatible with \"server\" option");
        System.out.println("       monitorOff     means monitoring data is NOT sent to client,");
        System.out.println("       password       is used to block clients without this password in their UDL's");
        System.out.println("       cloudpassword  is used to join a password-protected cloud or to allow");
        System.out.println("                      servers with this password to join this cloud");
        System.out.println("       lowRegimeSize  for clients of \"regime=low\" type, this sets the number of");
        System.out.println("                      clients serviced by a single thread");
        System.out.println();
    }

    /**
     * Run as a stand-alone application.
     * @param args arguments.
     */
    public static void main(String[] args) {

        int debug = cMsgConstants.debugNone;
        int port = 0, udpPort = 0, domainPort=0;
        int clientsMax = cMsgConstants.regimeLowMaxClients;
        boolean standAlone    = false;
        boolean monitorOff    = false;
        String serverToJoin   = null;
        String cloudPassword  = null;
        String clientPassword = null;

        if (args.length > 0) {
            usage();
            System.exit(-1);
        }

        // First check to see if debug level, port number, or timeordering
        // was set on the command line. This can be done, while ignoring case,
        // by scanning through all the properties.
        for (Iterator i = System.getProperties().keySet().iterator(); i.hasNext();) {
            String s = (String) i.next();

            if (s.contains(".")) {
                continue;
            }

            if (s.equalsIgnoreCase("port")) {
                try {
                    port = Integer.parseInt(System.getProperty(s));
                }
                catch (NumberFormatException e) {
                    System.out.println("\nBad port number specified");
                    usage();
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
            if (s.equalsIgnoreCase("udp")) {
                try {
                    udpPort = Integer.parseInt(System.getProperty(s));
                }
                catch (NumberFormatException e) {
                    System.out.println("\nBad port number specified");
                    usage();
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
            if (s.equalsIgnoreCase("domainPort")) {
                try {
                    domainPort = Integer.parseInt(System.getProperty(s));
                }
                catch (NumberFormatException e) {
                    System.out.println("\nBad domain server port number specified");
                    usage();
                    e.printStackTrace();
                    System.exit(-1);
                }
                if (domainPort > 65535 || domainPort < 1024) {
                    System.out.println("\nBad domain server port number specified");
                    usage();
                    System.exit(-1);
                }
            }
           if (s.equalsIgnoreCase("debug")) {
                String arg = System.getProperty(s);

                if (arg.equalsIgnoreCase("info")) {
                    debug = cMsgConstants.debugInfo;
                }
                else if (arg.equalsIgnoreCase("warn")) {
                    debug = cMsgConstants.debugWarn;
                }
                else if (arg.equalsIgnoreCase("error")) {
                    debug = cMsgConstants.debugError;
                }
                else if (arg.equalsIgnoreCase("severe")) {
                    debug = cMsgConstants.debugSevere;
                }
                else if (arg.equalsIgnoreCase("none")) {
                    debug = cMsgConstants.debugNone;
                }
                else {
                    System.out.println("\nBad debug value");
                    usage();
                    System.exit(-1);
                }
            } else if (s.equalsIgnoreCase("password")) {
                clientPassword = System.getProperty(s);
            }
        }

        // create server object
        cMsgNameServer server = new cMsgNameServer(port, domainPort, udpPort,
                                                   standAlone, monitorOff,
                                                   clientPassword, cloudPassword, serverToJoin,
                                                   debug, clientsMax);

        // start server
        server.startServer();
    }


    //-------------------------------------------------------------------
    // IExecutor Interface methods so it can be started/stopped remotely
    //-------------------------------------------------------------------
    public void startItUp() {
        startServer();
    }

    public void shutItDown() {
        shutdown();
    }

    public void waitUntilDone() throws InterruptedException {
        join();
    }
    //-------------------------------------------------------------


    /**
     * Method to start up this server.
     */
    public void startServer() {

        // Start xMsg pub/sub proxy
        if (debug >= cMsgConstants.debugInfo) {
            System.out.println(">> NS: Start xMsg proxy server");
        }
        try {
            xMsgProxyAddress address = new xMsgProxyAddress("0.0.0.0", domainServerPort);
            this.proxy = new xMsgProxy(xMsgContext.getInstance(), address);
            this.proxy.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // start this name server accepting client connections
        start();

        // start UDP listening thread for multicasters trying to connect
        if (debug >= cMsgConstants.debugInfo) {
            System.out.println(">> NS: Start multicast thd on port "  + multicastPort);
        }
        multicastThread = new cMsgMulticastListeningThread(this, port, multicastPort, multicastSocket,
                                                           clientPassword, debug);
        multicastThread.start();

        // Wait until these 2 listening threads have successfully started before continuing.
        // The Afecs platform starts a cmsg name server and immediately does a connect to it
        // in the same JVM. In such cases we need to avoid a race condition whereby a client
        // connects while these threads are still being started.
        try {
            boolean threadsStarted = listeningThreadsStartedSignal.await(20L, TimeUnit.SECONDS);
            if (!threadsStarted) {
                System.out.println(">> **** cMsg server NOT started due to listening theads taking too long to start **** <<");
                shutdown();
                return;
            }
        }
        catch (InterruptedException e) {
            System.out.println(">> **** cMsg server NOT started due to interrupt **** <<");
            shutdown();
            return;
        }

    }


    /**
     * Implement IExecutorThread interface so cMsgNameServer can be
     * run using the Commander/Executor framework.
     */
    public void cleanUp() {
        shutdown();
    }

    /**
     * Method to gracefully shutdown all threads associated with this server
     * and to clean up.
     */
    synchronized void shutdown() {
        // Shutdown this object's listening thread
        setKillAllThreads(true);

        // Shutdown UDP listening thread
        multicastThread.killThread();

        xMsgContext.getInstance().destroy();
        this.proxy.shutdown();
    }


    /** This method is executed as a thread. */
    public void run() {
        if (debug >= cMsgConstants.debugInfo) {
            System.out.println(">> NS: Running Name Server at " + (new Date()) );
        }

        // Prepare success response
        try {
            ByteArrayOutputStream successBaos = new ByteArrayOutputStream(128);
            DataOutputStream successOut = new DataOutputStream(successBaos);

            // send ok back as acknowledgment
            successOut.writeInt(cMsgConstants.ok);

            // send cMsg domain port contact info back to client
            successOut.writeInt(domainServerPort);
            successOut.flush();
            this.successResponse = successBaos.toByteArray();
            successBaos.close();
        } catch (IOException e) {
           System.out.println("cMsgServerName::run() - Something went wrong during success message creation");
        }
        // Prepare error response
        try {
            ByteArrayOutputStream errorBaos = new ByteArrayOutputStream(128);
            DataOutputStream errorOut = new DataOutputStream(errorBaos);

            errorOut.writeInt(cMsgConstants.errorBadFormat);
            String errorMsg = "Incorrect format";
            errorOut.writeInt(errorMsg.length());
            errorOut.write(errorMsg.getBytes("US-ASCII"));

            errorOut.flush();
            errorOut.close();
            this.errorResponse = errorBaos.toByteArray();
            errorBaos.close();
        } catch (IOException e) {
            System.out.println("cMsgServerName::run() - Something went wrong during error message creation");
        }


        /* Direct buffer for reading 3 magic ints with nonblocking IO. */
        int BYTES_TO_READ = 12;
        ByteBuffer buffer = ByteBuffer.allocateDirect(BYTES_TO_READ);

        Selector selector = null;

        try {
            // get things ready for a select call
            selector = Selector.open();

            // set nonblocking mode for the listening socket
            serverChannel.configureBlocking(false);

            // register the channel with the selector for accepts
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            // Tell whoever is waiting for this server to start, that
            // it has now started. Actually there is a slight race
            // condition as it is not actually started until the select
            // statement below is executed. But it'll be OK since the thread
            // which is waiting must first create a bridge to another
            // server who then must make a reciprocal connection to this
            // server (right here as a matter of fact).
            listeningThreadsStartedSignal.countDown();

            while (true) {
                // 3 second timeout
                int n = selector.select(3000);

                // if no channels (sockets) are ready, listen some more
                if (n == 0) {
                    // but first check to see if we've been commanded to die
                    if (getKillAllThreads()) {
                        return;
                    }
                    continue;
                }

                // get an iterator of selected keys (ready sockets)
                Iterator it = selector.selectedKeys().iterator();

                // look at each key
                keyLoop:
                while (it.hasNext()) {
                    SelectionKey key = (SelectionKey) it.next();

                    // is this a new connection coming in?
                    if (key.isValid() && key.isAcceptable()) {
                        ServerSocketChannel server = (ServerSocketChannel) key.channel();
                        // accept the connection from the client
                        SocketChannel channel = server.accept();

                        // Check to see if this is a legitimate cMsg client or some imposter.
                        // Don't want to block on read here since it may not be a cMsg client
                        // and may block forever - tying up the server.
                        int bytes, bytesRead=0, loops=0;
                        buffer.clear();
                        buffer.limit(BYTES_TO_READ);
                        channel.configureBlocking(false);

                        // The "try" protects against a bad client - allows recovery of server
                        try {
                            // read magic numbers
                            while (bytesRead < BYTES_TO_READ) {
//System.out.println("  try reading rest of Buffer");
//System.out.println("  Buffer capacity = " + buffer.capacity() + ", limit = " + buffer.limit()
//                   + ", position = " + buffer.position() );
                                bytes = channel.read(buffer);
                                // for End-of-stream ...
                                if (bytes == -1) {
//System.out.println("cMsgNameServer: closing bad channel");
                                    channel.close();
                                    it.remove();
                                    continue keyLoop;
                                }
                                bytesRead += bytes;
//System.out.println("  bytes read = " + bytesRead);

                                // if we've read everything, look to see if it's sent the magic #s
                                if (bytesRead >= BYTES_TO_READ) {
                                    buffer.flip();
                                    int magic1 = buffer.getInt();
                                    int magic2 = buffer.getInt();
                                    int magic3 = buffer.getInt();
                                    if (magic1 != cMsgNetworkConstants.magicNumbers[0] ||
                                        magic2 != cMsgNetworkConstants.magicNumbers[1] ||
                                        magic3 != cMsgNetworkConstants.magicNumbers[2])  {
//System.out.println("cMsgNameServer:  Magic numbers did NOT match, send response");
                                        // For old versions of cMsg (1 and 2), it's expecting a response
                                        channel.write(ByteBuffer.wrap(errorResponse));
//System.out.println("cMsgNameServer: closing bad channel");
                                        channel.close();
                                        it.remove();
                                        continue keyLoop;
                                    }
                                }
                                else {
                                    // give client 50 loops (.5 sec total) to send its stuff, else no deal
                                    if (++loops > 50) {
//System.out.println("cMsgNameServer: taking too long, closing bad channel");
                                        channel.write(ByteBuffer.wrap(errorResponse));
                                        channel.close();
                                        it.remove();
                                        continue keyLoop;
                                    }
                                    try { Thread.sleep(10); }
                                    catch (InterruptedException e) { }
                                }
                            }
                        }
                        catch (IOException e) {
                            // Recover from bad client
                            channel.close();
                            it.remove();
                            continue keyLoop;
                        }

//System.out.println("cMsgNameServer:  Magic numbers did match");
                        // back to using streams
                        channel.configureBlocking(true);
                        // set socket options
                        Socket socket = channel.socket();
                        // Set tcpNoDelay so no packets are delayed
                        socket.setTcpNoDelay(true);
                        // set recv buffer size
                        socket.setReceiveBufferSize(4096);

                        // start up client handling thread & store reference
                        handlerThreads.add(new ClientHandler(channel));
//System.out.println("handlerThd size = " + handlerThreads.size());

                        if (debug >= cMsgConstants.debugInfo) {
                            System.out.println(">> NS: new connection");
                        }
                    }
                    // remove key from selected set since it's been handled
                    it.remove();
                }
            }
        }
        catch (IOException ex) {
            //if (debug >= cMsgConstants.debugError) {
System.out.println("Main server IO error");
                ex.printStackTrace();
            //}
        }
        finally {
            try {serverChannel.close();} catch (IOException e) {}
            try {selector.close();}      catch (IOException e) {}
        }

        if (debug >= cMsgConstants.debugInfo) {
            System.out.println("\n>> NS: Quitting Name Server");
        }
    }


    /** Class to handle a socket connection to the client of which there may be many. */
    private class ClientHandler extends Thread {
        /** Socket channel to client. */
        SocketChannel channel;

        /** Buffered input communication streams for efficiency. */
        DataInputStream  in;
        /** Buffered output communication streams for efficiency. */
        DataOutputStream out;

        /**
         * Constructor.
         * @param channel socket channel to client
         */
        ClientHandler(SocketChannel channel) {
            this.channel = channel;
            this.start();
        }


        /**
          * This method handles all communication between a cMsg user
          * and this name server for that domain.
          * Note to those who would make changes in the protocol, keep the first three
          * ints the same. That way the server can reliably check for mismatched versions.
          */
         public void run() {     

            try {
                // buffered communication streams for efficiency
                in  = new DataInputStream(new BufferedInputStream(channel.socket().getInputStream(), 4096));
                out = new DataOutputStream(new BufferedOutputStream(channel.socket().getOutputStream(), 2048));

                // cMsg version
                int version = in.readInt();
                // message id
                int msgId = in.readInt();
                // password len
                int lengthPassword = in.readInt();
                // password
                byte[] passwordBytes = new byte[lengthPassword];
                in.readFully(passwordBytes);
                String password = new String(passwordBytes, "US-ASCII");

                // immediately check if this domain server is different cMsg version than client
                if (version != cMsgConstants.version) {
                    if (debug >= cMsgConstants.debugError) {
                        System.out.println("version mismatch, client(" + version + ") != server(" +
                                            cMsgConstants.version + ")");
                    }
                    out.writeInt(cMsgConstants.errorDifferentVersion);
                    String s = "version mismatch, client(" + version + ") != server(" +
                                cMsgConstants.version + ")";
                    out.writeInt(s.length());
                    try { out.write(s.getBytes("US-ASCII")); }
                    catch (UnsupportedEncodingException e) {}

                    out.flush();
                    return;
                }
                
                // validate msgType
                if (msgId != cMsgNetworkConstants.cMsgDomainTCP) {
                    if (debug >= cMsgConstants.debugError) {
                        System.out.println("cMsg name server: can't understand your message -> " + msgId);
                    }
                    out.writeInt(cMsgConstants.errorIllegalMessageType);
                    String s = "msg type != cMsgNetworkConstants.cMsgDomainTCP";
                    out.writeInt(s.length());
                    try { out.write(s.getBytes("US-ASCII")); }
                    catch (UnsupportedEncodingException e) {}

                    out.flush();
                    return;
                }

                // if the client does not provide the correct password if required, return an error
                if (clientPassword != null) {
                    if (debug >= cMsgConstants.debugInfo) {
                        System.out.println("  local password = " + clientPassword);
                        System.out.println("  given password = " + password);
                    }
                    if (password.length() < 1 || !clientPassword.equals(password)) {
                        if (debug >= cMsgConstants.debugError) {
                            System.out.println("  wrong password sent");
                        }
                        out.writeInt(cMsgConstants.errorWrongPassword);
                        String s = "wrong password given";
                        out.writeInt(s.length());
                        try {
                            out.write(s.getBytes("US-ASCII"));
                        }
                        catch (UnsupportedEncodingException e) {
                        }
                        out.flush();
                        return;
                    }
                }

                // all packet tests passed so send back proxy server details
                out.write(successResponse);
                out.flush();           
            }
            catch (IOException ex) {
                if (debug >= cMsgConstants.debugError) {
                    System.out.println("cMsgNameServer's Client thread: IO error in talking to client");
                }
                 try {
                    // Send error response back to client before closing
                    out.write(errorResponse);
                    out.flush();
                } catch (IOException sendError) {
                    System.err.println("Error sending error response to client: " + sendError.getMessage());
                    sendError.printStackTrace();
                }
            }
            finally {
                handlerThreads.remove(this);
                // we are done with the channel
                try {in.close();}      catch (IOException ex) {}
                try {out.close();}     catch (IOException ex) {}
                try {channel.close();} catch (IOException ex) {}
            }
         }       
    }
}