/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgSocketOption;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 *    xMsg base class that provides methods
 *    for organizing pub/sub communications.
 *
 *    This class provides a private database
 *    of xMsgCommunications for publishing
 *    and/or subscribing messages without
 *    requesting registration information from
 *    the local registrar services.
 *
 *    This class also provides a thread pool for
 *    servicing received messages (as a result of
 *    a subscription) in separate threads.
 * </p>
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsg {

    /** The unique identifier of this actor. */
    protected final String myName;
    /**
     * Database of stored proxy connections.
     */
    private final Map<xMsgAddress, xMsgConnection> connections = new HashMap<>();
    /**
     * Database of stored registration server driver objects
     */
    private final Map<String, xMsgRegDriver> regDrivers = new HashMap<>();
    /** Fixed size thread pool. */
    private final ThreadPoolExecutor threadPool;
    /**
     * Default proxy host IP
     */
    private String myProxyIp;
    /**
     * Default proxy port
     */
    private int myProxyPort;
    /**
     * Default thread pool size.
     */
    private int myPoolSize;
    /** 0MQ context object */
    private ZContext context = xMsgContext.getContext();

    /** Default socket options.*/
    private xMsgSocketOption defaultSocketSetup;


    /**
     *
     * @param name
     * @param proxyIp
     * @param proxyPort
     * @param poolSize
     * @throws IOException
     */
    public xMsg(String name, String proxyIp, int proxyPort, int poolSize) throws IOException {

        // We need to have a name for an actor
        this.myName = name;

        this.myProxyIp = proxyIp;
        this.myProxyPort = proxyPort;
        this.myPoolSize = poolSize;


        // create fixed size thread pool
        this.threadPool = xMsgUtil.newFixedThreadPool(myPoolSize, name);

        // default pub/sub socket options
        defaultSocketSetup = new xMsgSocketOption() {

            @Override
            public void preConnection(Socket socket) {
                socket.setRcvHWM(0);
                socket.setSndHWM(0);
            }

            @Override
            public void postConnection() { }
        };

        // fix default linger
        this.context.setLinger(-1);

        // connect to the default proxy
        reConnect(myProxyIp, myProxyPort);
    }

    /**
     *
     * @param name
     * @param poolSize
     * @throws IOException
     */
    public xMsg(String name, int poolSize) throws IOException {
        this(name, xMsgUtil.localhost(), xMsgConstants.DEFAULT_PORT.getIntValue(), poolSize);
    }

    /**
     *
     * @param name
     * @throws IOException
     */
    public xMsg(String name) throws IOException {
        this(name, xMsgUtil.localhost(),
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
    }

    /**
     *
     * @return
     */
    public String getName() {
        return myName;
    }

    /**
     *
     * @return
     */
    public String getProxyIp() {
        return myProxyIp;
    }

    /**
     *
     * @return
     */
    public int getProxyPort() {
        return myProxyPort;
    }

    /**
     *
     * @return
     */
    public int getPoolSize() {
        return myPoolSize;
    }

    /**
     * @return
     */
    public xMsgSocketOption getDefaultSocketSetup() {
        return defaultSocketSetup;
    }

    /**
     * @param defaultSocketSetup
     */
    public void setDefaultSocketSetup(xMsgSocketOption defaultSocketSetup) {
        this.defaultSocketSetup = defaultSocketSetup;
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @return
     */
    public xMsgConnection reConnect(String proxyIp, int proxyPort) {
        return connectAndStore(new xMsgAddress(proxyIp, proxyPort), defaultSocketSetup);
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @return
     */
    public xMsgConnection clearConnect(String proxyIp, int proxyPort) {
        xMsgAddress address = new xMsgAddress(proxyIp,proxyPort);
        if (connections.containsKey(address)) {
            disconnect(connections.get(address));
        }
        return reConnect(proxyIp, proxyPort);
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @return
     */
    public xMsgConnection getConnection(String proxyIp, int proxyPort) {
        xMsgAddress address = new xMsgAddress(proxyIp, proxyPort);
        return connections.get(address);
    }

    /**
     * @return
     */
    public xMsgConnection getDefaultConnection() {
        return getConnection(myProxyIp, myProxyPort);
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @return
     */
    public xMsgConnection connect(String proxyIp, int proxyPort) {
        xMsgAddress address = new xMsgAddress(proxyIp, proxyPort);
        return createConnection(address, defaultSocketSetup);
    }

    /**
     *
     * @param connection
     */
    public void disconnect(xMsgConnection connection) {
        context.destroySocket(connection.getPubSock());
        context.destroySocket(connection.getSubSock());
        connections.remove(connection.getAddress());
    }


    /**
     *
     */
    public void destruct() {
        context.destroy();
        threadPool.shutdown();
        connections.clear();
    }

    /**
     *
     * @param linger
     */
    public void destruct(int linger) {
        context.setLinger(linger);
        destruct();
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void registerAsPublisher(String regServerIp,
                                    int regServPort,
                                    xMsgTopic topic,
                                    String description)
            throws xMsgRegistrationException, IOException {
        register(regServerIp, regServPort, topic, description, true);
    }

    /**
     *
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void registerAsPublisher(xMsgTopic topic,
                                    String description)
            throws xMsgRegistrationException, IOException {
        register(xMsgUtil.localhost(), xMsgConstants.REGISTRAR_PORT.getIntValue(), topic, description, true);
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void registerAsSubscriber(String regServerIp,
                                     int regServPort,
                                     xMsgTopic topic,
                                     String description)
            throws xMsgRegistrationException, IOException {
        register(regServerIp, regServPort, topic, description, false);
    }

    /**
     *
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void registerAsSubscriber(xMsgTopic topic,
                                     String description)
            throws xMsgRegistrationException, IOException {
        register(xMsgUtil.localhost(), xMsgConstants.REGISTRAR_PORT.getIntValue(), topic, description, false);
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removePublisherRegistration(String regServerIp,
                                            int regServPort,
                                            xMsgTopic topic,
                                            String description)
            throws xMsgRegistrationException, IOException {
        removeRegistration(regServerIp, regServPort, topic, description, true);
    }

    /**
     *
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removePublisherRegistration(xMsgTopic topic,
                                            String description)
            throws xMsgRegistrationException, IOException {
        removeRegistration(xMsgUtil.localhost(), xMsgConstants.REGISTRAR_PORT.getIntValue(), topic, description, true);
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removeSubscriberRegistration(String regServerIp,
                                             int regServPort,
                                             xMsgTopic topic,
                                             String description)
            throws xMsgRegistrationException, IOException {
        removeRegistration(regServerIp, regServPort, topic, description, false);
    }

    /**
     *
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removeSubscriberRegistration(xMsgTopic topic,
                                             String description)
            throws xMsgRegistrationException, IOException {
        removeRegistration(xMsgUtil.localhost(), xMsgConstants.REGISTRAR_PORT.getIntValue(), topic, description, false);
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findPublishers(String regServerIp,
                                                int regServPort,
                                                xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(regServerIp, regServPort,topic, true);
    }

    /**
     *
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findPublishers(xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(myProxyIp, myProxyPort,topic, true);
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findSubscribers(String regServerIp,
                                                 int regServPort,
                                                 xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(regServerIp, regServPort,topic, false);
    }

    /**
     *
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findSubscribers(xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(myProxyIp, myProxyPort,topic, false);
    }

    /**
     *
     * @param con
     * @param msg
     * @throws xMsgException
     */
    public void publish(xMsgConnection con, xMsgMessage msg) throws xMsgException {
        try {
            _publish(con, msg, -1);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    public void publish(String proxyIp, int proxyPort,
                        xMsgTopic topic, String mimeType,
                        Object data)
            throws xMsgException, IOException {
        try {
            _publish(proxyIp, proxyPort, topic, mimeType, data, -1);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param proxyIp
     * @param proxyPort
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    public void publish(String proxyIp, int proxyPort,
                        xMsgTopic topic,
                        Object data)
            throws xMsgException, IOException {
        try {
            _publish(proxyIp, proxyPort, topic, null, data, -1);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param topic
     * @param mimeType
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    public void publish(xMsgTopic topic, String mimeType,
                        Object data)
            throws xMsgException, IOException {
        try {
            _publish(myProxyIp, myProxyPort, topic, mimeType, data, -1);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param topic
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    public void publish(xMsgTopic topic,
                        Object data)
            throws xMsgException, IOException {
        try {
            _publish(myProxyIp, myProxyPort, topic, null, data, -1);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param con
     * @param msg
     * @param timeout
     * @return
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage syncPublish(xMsgConnection con,
                                   xMsgMessage msg,
                                   int timeout) throws xMsgException, TimeoutException {
        return _publish(con, msg, timeout);
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @param topic
     * @param mimeType
     * @param data
     * @param timeout
     * @return
     * @throws xMsgException
     * @throws IOException
     * @throws TimeoutException
     */
    public xMsgMessage syncPublish(String proxyIp, int proxyPort,
                                   xMsgTopic topic, String mimeType,
                                   Object data, int timeout)
            throws xMsgException, IOException, TimeoutException {
        return _publish(proxyIp, proxyPort, topic, mimeType, data, timeout);
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @param topic
     * @param data
     * @param timeout
     * @return
     * @throws xMsgException
     * @throws IOException
     * @throws TimeoutException
     */
    public xMsgMessage syncPublish(String proxyIp, int proxyPort,
                                   xMsgTopic topic,
                                   Object data, int timeout)
            throws xMsgException, IOException, TimeoutException {
        return _publish(proxyIp, proxyPort, topic, null, data, timeout);
    }

    /**
     * @param topic
     * @param mimeType
     * @param data
     * @param timeout
     * @return
     * @throws xMsgException
     * @throws IOException
     * @throws TimeoutException
     */
    public xMsgMessage syncPublish(xMsgTopic topic, String mimeType,
                                   Object data, int timeout)
            throws xMsgException, IOException, TimeoutException {
        return _publish(myProxyIp, myProxyPort, topic, mimeType, data, timeout);
    }

    /**
     *
     * @param topic
     * @param data
     * @param timeout
     * @return
     * @throws xMsgException
     * @throws IOException
     * @throws TimeoutException
     */
    public xMsgMessage syncPublish(xMsgTopic topic,
                                   Object data, int timeout)
            throws xMsgException, IOException, TimeoutException {
        return _publish(myProxyIp, myProxyPort, topic, null, data, timeout);
    }

    /**
     *
     * @param proxyIp
     * @param proxyPort
     * @param topic
     * @param cb
     * @return
     * @throws xMsgException
     */
    public xMsgSubscription subscribe(final String proxyIp, final int proxyPort,
                                      final xMsgTopic topic,
                                      final xMsgCallBack cb)
    throws xMsgException {

        // get connection to the proxy
        final xMsgConnection con = reConnect(proxyIp, proxyPort);

        // get pub socket
        Socket sock = con.getSubSock();
        if (sock == null) {
            throw new xMsgException("xMsg-Error: null sub socket");
        }

        String name = "sub-" + myName + "-" + con.getAddress() + "-" + topic;

        xMsgSubscription sHandle = new xMsgSubscription(name, con, topic) {
            @Override
            public void handle(ZMsg inputMsg) throws xMsgException, IOException {
                final xMsgMessage callbackMsg = new xMsgMessage(inputMsg);
                callUserCallBack(con, cb, callbackMsg);
            }
        };

        sHandle.start();
        return sHandle;
    }

    /**
     *
     * @param topic
     * @param cb
     * @return
     * @throws xMsgException
     */
    public xMsgSubscription subscribe(final xMsgTopic topic,
                                      final xMsgCallBack cb)
            throws xMsgException {
        return subscribe(myProxyIp, myProxyPort, topic, cb);

    }

    /**
     * <p>
     *     Un-subscribes  subscription. This will stop
     *     thread and perform xmq un-subscribe
     * </p>
     * @param handle SubscribeHandler object reference
     * @throws xMsgException
     */
    public void unsubscribe(xMsgSubscription handle)
            throws xMsgException {
        handle.stop();
    }

    // ..............................................................//
    //                        Private section
    // ..............................................................//

    /**
     *
     * @param topic
     * @param description
     * @return
     */
    private Builder createRegistration(xMsgTopic topic, String description) {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(myName);
        regb.setHost(myProxyIp);
        regb.setPort(myProxyPort);
        regb.setDomain(topic.domain());
        regb.setSubject(topic.subject());
        regb.setType(topic.type());
        regb.setDescription(description);
        return regb;
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @param description
     * @param isPublisher
     * @throws xMsgRegistrationException
     */
    private void register(String regServerIp,
                          int regServPort,
                          xMsgTopic topic,
                          String description,
                          boolean isPublisher)
            throws xMsgRegistrationException {

        // Define the key for the registration server connection
        // map, for storing and reusing registrar connections
        String regDriverKey = regServerIp + "_" + regServPort;
        xMsgRegDriver regDriver;
        if (regDrivers.containsKey(regDriverKey)) {
            regDriver = regDrivers.get(regDriverKey);
        } else {
            // create the registration driver object
            regDriver = new xMsgRegDriver(context, regServerIp, regServPort);

            // put into the map of regDrivers
            regDrivers.put(regDriverKey, regDriver);
        }
        xMsgRegistration.Builder regb = createRegistration(topic, description);
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        xMsgRegistration regData = regb.build();
        regDriver.register(regData, isPublisher);
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @param description
     * @param isPublisher
     * @throws xMsgRegistrationException
     */
    private void removeRegistration(String regServerIp,
                                    int regServPort,
                                    xMsgTopic topic,
                                    String description,
                                    boolean isPublisher)
            throws xMsgRegistrationException {

        String regDriverKey = regServerIp + "_" + regServPort;
        xMsgRegDriver regDriver;
        if (regDrivers.containsKey(regDriverKey)) {
            regDriver = regDrivers.get(regDriverKey);
        } else {
            // create the registration driver object
            regDriver = new xMsgRegDriver(context, regServerIp, regServPort);

            // put into the map of regDrivers
            regDrivers.put(regDriverKey, regDriver);
        }
        xMsgRegistration.Builder regb = createRegistration(topic, description);
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        xMsgRegistration regData = regb.build();
        regDriver.removeRegistration(regData, isPublisher);
    }

    /**
     *
     * @param regServerIp
     * @param regServPort
     * @param topic
     * @param isPublisher
     * @return
     * @throws xMsgRegistrationException
     */
    private Set<xMsgRegistration> findRegistration(String regServerIp,
                                                   int regServPort,
                                                   xMsgTopic topic,
                                                   boolean isPublisher)
            throws xMsgRegistrationException {

        // Define the key for the registration server connection
        // map, for storing and reusing registrar connections
        String regDriverKey = regServerIp + "_" + regServPort;
        xMsgRegDriver regDriver;
        if (regDrivers.containsKey(regDriverKey)) {
            regDriver = regDrivers.get(regDriverKey);
        } else {
            // create the registration driver object
            regDriver = new xMsgRegDriver(context, regServerIp, regServPort);

            // put into the map of regDrivers
            regDrivers.put(regDriverKey, regDriver);
        }
        xMsgRegistration.Builder regb = createRegistration(topic, "");
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        xMsgRegistration regData = regb.build();
        return regDriver.findRegistration(regData, isPublisher);
    }


    /**
     *
     * @param address
     * @param setup
     * @return
     */
    private xMsgConnection createConnection(xMsgAddress address, xMsgSocketOption setup) {
        Socket pubSock = context.createSocket(ZMQ.PUB);
        Socket subSock = context.createSocket(ZMQ.SUB);
        setup.preConnection(pubSock);
        setup.preConnection(subSock);

        int pubPort = address.getPort();
        int subPort = pubPort + 1;
        pubSock.connect("tcp://" + address.getHost() + ":" + pubPort);
        subSock.connect("tcp://" + address.getHost() + ":" + subPort);
        setup.postConnection();

        xMsgConnection connection = new xMsgConnection();
        connection.setAddress(address);
        connection.setPubSock(pubSock);
        connection.setSubSock(subSock);

        return connection;
    }

    /**
     *
     * @param address
     * @param setup
     * @return
     */
    private xMsgConnection connectAndStore(xMsgAddress address, xMsgSocketOption setup) {
        /*
         * First check to see if we have already established connection
         * to this address
         */
        if (connections.containsKey(address)) {
            return connections.get(address);
        } else {
            /*
             * Otherwise create sockets to the requested address, and store the
             * created connection object for the future use. Return the
             * reference to the connection object
             */
            xMsgConnection connection = createConnection(address, setup);
            connections.put(address, connection);
            return connection;
        }
    }

    /**
     * @param con
     * @param msg
     * @param timeout
     * @return
     * @throws xMsgException
     * @throws TimeoutException
     */
    private xMsgMessage _publish(xMsgConnection con, xMsgMessage msg, int timeout) throws xMsgException, TimeoutException {

        SyncSendCallBack cb = null;
        // get pub socket
        Socket sock = con.getPubSock();

        if (timeout > 0) {
            // address/topic where the subscriber should send the result
            String returnAddress = "return:" + (int) (Math.random() * 100.0);

            // set the return address as replyTo in the xMsgMessage
            msg.getMetaData().setReplyTo(returnAddress);


            // subscribe to the returnAddress
            cb = new SyncSendCallBack();
            xMsgSubscription sh = subscribe(con.getAddress().getHost(), con.getAddress().getPort(),
                    xMsgTopic.wrap(returnAddress), cb);
            cb.setSubscriptionHandler(sh);
        }

        // send topic, sender, followed by the metadata and data
        ZMsg outputMsg = msg.serialize();
        try {
            outputMsg.send(sock);
        } catch (ZMQException e) {
            throw new xMsgException("xMsg-Error: publishing failed. " + e.getMessage());
        } finally {
            outputMsg.destroy();
        }

        if (timeout > 0) {
            // wait for the response
            int t = 0;
            while (cb.recvMsg == null && t < timeout * 1000) {
                t++;
                xMsgUtil.sleep(1);
            }
            if (t >= timeout * 1000) {
                throw new TimeoutException("xMsg-Error: no response for time_out = " + t);
            }
            msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());
            return cb.recvMsg;
        }
        return null;
    }

    /**
     * @param proxyIp
     * @param proxyPort
     * @param mimeType
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    private xMsgMessage _publish(String proxyIp, int proxyPort,
                                 xMsgTopic topic,
                                 String mimeType, Object data, int timeout)
            throws xMsgException, IOException, TimeoutException {

        SyncSendCallBack cb = null;

        // get connection to the proxy
        xMsgConnection con = reConnect(proxyIp, proxyPort);

        // get pub socket
        Socket sock = con.getPubSock();
        if (sock == null) {
            throw new xMsgException("xMsg-Error: null pub socket");
        }

        // create a message
        xMsgMessage msg;
        if (mimeType != null) {
            msg = new xMsgMessage(topic, mimeType, data);
        } else {
            msg = new xMsgMessage(topic, data);
        }

        if (timeout > 0) {
            // address/topic where the subscriber should send the result
            String returnAddress = "return:" + (int) (Math.random() * 100.0);

            // set the return address as replyTo in the xMsgMessage
            msg.getMetaData().setReplyTo(returnAddress);


            // subscribe to the returnAddress
            cb = new SyncSendCallBack();
            xMsgSubscription sh = subscribe(con.getAddress().getHost(), con.getAddress().getPort(),
                    xMsgTopic.wrap(returnAddress), cb);
            cb.setSubscriptionHandler(sh);
        }

        // send topic, sender, followed by the metadata and data
        ZMsg outputMsg = msg.serialize();
        try {
            outputMsg.send(sock);
        } catch (ZMQException e) {
            throw new xMsgException("xMsg-Error: publishing failed. " + e.getMessage());
        } finally {
            outputMsg.destroy();
        }
        if (timeout > 0) {
            // wait for the response
            int t = 0;
            while (cb.recvMsg == null && t < timeout * 1000) {
                t++;
                xMsgUtil.sleep(1);
            }
            if (t >= timeout * 1000) {
                throw new TimeoutException("xMsg-Error: no response for time_out = " + t);
            }
            msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());
            return cb.recvMsg;
        }
        return null;
    }

    /**
     * @param connection
     * @param callback
     * @param callbackMsg
     * @throws xMsgException
     * @throws IOException
     */
    private void callUserCallBack(final xMsgConnection connection,
                                  final xMsgCallBack callback,
                                  final xMsgMessage callbackMsg)
            throws xMsgException, IOException {

        // Check if it is sync request
        // sync request
        String requester = callbackMsg.getMetaData().getReplyTo();
        if (!requester.equals(xMsgConstants.UNDEFINED.getStringValue())) {
            xMsgMessage rm = callback.callback(callbackMsg);
            if (rm != null) {
                rm.setTopic(xMsgTopic.wrap(requester));
                publish(connection, rm);
            }
        } else {
            // async request
            threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    callback.callback(callbackMsg);
                }
            });
        }
    }


    /**
     * Private inner class used to organize sync send/publish communications.
     */
    private class SyncSendCallBack implements xMsgCallBack {

        public xMsgMessage recvMsg = null;

        private xMsgSubscription handler = null;

        public void setSubscriptionHandler(xMsgSubscription handler) {
            this.handler = handler;
        }

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
            recvMsg = msg;
            try {
                if (handler != null) {
                    unsubscribe(handler);
                }
            } catch (xMsgException e) {
                e.printStackTrace();
            }

            return recvMsg;
        }
    }
}
