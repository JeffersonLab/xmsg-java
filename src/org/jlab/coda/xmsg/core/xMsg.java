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
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionOption;
import org.jlab.coda.xmsg.net.xMsgPrxAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.io.IOException;
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

    /** Fixed size thread pool. */
    private final ThreadPoolExecutor threadPool;

    /**
     * Default thread pool size.
     */
    private int myPoolSize;

    private String defaultProxyHost;

    private int defaultProxyPort;

    private String defaultRegistrarHost;

    private int defaultRegistrarPort;

    /** 0MQ context object */
    private ZContext context = xMsgContext.getContext();

    /** Default socket options.*/
    private xMsgConnectionOption defaultConnectionOption;


    /**
     *
     * @param name
     * @param poolSize
     * @throws IOException
     */
    public xMsg(String name, int poolSize) throws IOException {
        this(name, xMsgUtil.localhost(), xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgUtil.localhost(), xMsgConstants.REGISTRAR_PORT.getIntValue(), poolSize);
        myPoolSize = poolSize;
        defaultProxyHost = xMsgUtil.localhost();
        defaultProxyPort = xMsgConstants.DEFAULT_PORT.getIntValue();
        defaultRegistrarHost = xMsgUtil.localhost();
        defaultRegistrarPort = xMsgConstants.REGISTRAR_PORT.getIntValue();
    }

    /**
     * @param name
     * @throws IOException
     */
    public xMsg(String name) throws IOException {
        this(name, xMsgUtil.localhost(), xMsgConstants.DEFAULT_PORT.getIntValue(),
                xMsgUtil.localhost(), xMsgConstants.REGISTRAR_PORT.getIntValue(),
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
        myPoolSize = xMsgConstants.DEFAULT_POOL_SIZE.getIntValue();
        defaultProxyHost = xMsgUtil.localhost();
        defaultProxyPort = xMsgConstants.DEFAULT_PORT.getIntValue();
        defaultRegistrarHost = xMsgUtil.localhost();
        defaultRegistrarPort = xMsgConstants.REGISTRAR_PORT.getIntValue();
    }

    /**
     * @param name
     * @param registrarHost
     * @param poolSize
     * @throws IOException
     */
    public xMsg(String name, String registrarHost, int poolSize) throws IOException {
        this(name, xMsgUtil.localhost(), xMsgConstants.DEFAULT_PORT.getIntValue(),
                registrarHost, xMsgConstants.REGISTRAR_PORT.getIntValue(), poolSize);
        myPoolSize = poolSize;
        defaultProxyHost = xMsgUtil.localhost();
        defaultProxyPort = xMsgConstants.DEFAULT_PORT.getIntValue();
        defaultRegistrarHost = registrarHost;
        defaultRegistrarPort = xMsgConstants.REGISTRAR_PORT.getIntValue();
    }

    /**
     * @param name
     * @param registrarHost
     * @throws IOException
     */
    public xMsg(String name, String registrarHost) throws IOException {
        this(name, xMsgUtil.localhost(), xMsgConstants.DEFAULT_PORT.getIntValue(),
                registrarHost, xMsgConstants.REGISTRAR_PORT.getIntValue(),
                xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
        myPoolSize = xMsgConstants.DEFAULT_POOL_SIZE.getIntValue();
        defaultProxyHost = xMsgUtil.localhost();
        defaultProxyPort = xMsgConstants.DEFAULT_PORT.getIntValue();
        defaultRegistrarHost = registrarHost;
        defaultRegistrarPort = xMsgConstants.REGISTRAR_PORT.getIntValue();
    }

    /**
     *
     * @param name
     * @param poolSize
     * @throws IOException
     */
    public xMsg(String name, String defaultProxyHost, int defaultProxyPort,
                String defaultRegistrarHost, int defaultRegistrarPort,
                int poolSize) throws IOException {

        // We need to have a name for an actor
        this.myName = name;

        this.myPoolSize = poolSize;
        this.defaultRegistrarHost = defaultRegistrarHost;
        this.defaultRegistrarPort = defaultRegistrarPort;
        this.defaultProxyHost = defaultProxyHost;
        this.defaultProxyPort = defaultProxyPort;

        // create fixed size thread pool
        this.threadPool = xMsgUtil.newFixedThreadPool(myPoolSize, name);

        // default pub/sub socket options
        defaultConnectionOption = new xMsgConnectionOption() {

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
    public int getPoolSize() {
        return myPoolSize;
    }

    /**
     * @param defaultConnectionOption
     */
    public void setDefaultConnectionOption(xMsgConnectionOption defaultConnectionOption) {
        this.defaultConnectionOption = defaultConnectionOption;
    }


    /**
     *
     * @param address
     * @return
     */
    public xMsgConnection connect(xMsgPrxAddress address) {
        return createConnection(address, defaultConnectionOption);
    }

    /**
     *
     * @param address
     * @param setUp
     * @return
     */
    public xMsgConnection connect(xMsgPrxAddress address, xMsgConnectionOption setUp) {
        return createConnection(address, setUp);
    }

    /**
     *
     * @param proxyHost
     * @return
     */
    public xMsgConnection connect(String proxyHost) {
        xMsgPrxAddress address = new xMsgPrxAddress(proxyHost);
        return createConnection(address, defaultConnectionOption);
    }

    public xMsgConnection connect() {
        xMsgPrxAddress address = new xMsgPrxAddress(defaultProxyHost, defaultProxyPort);
        return createConnection(address, defaultConnectionOption);
    }

    /**
     *
     * @param connection
     */
    public void disconnect(xMsgConnection connection) {
        context.destroySocket(connection.getPubSock());
        context.destroySocket(connection.getSubSock());
    }


    /**
     *
     */
    public void destruct() {
        context.destroy();
        threadPool.shutdown();
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
     * @param address
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void registerAsPublisher(xMsgRegAddress address,
                                    xMsgTopic topic,
                                    String description)
            throws xMsgRegistrationException, IOException {
        register(address.getHost(), address.getPort(), topic, description, true);
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
        register(defaultRegistrarHost, defaultRegistrarPort, topic, description, true);
    }

    /**
     *
     * @param address
     * @param topic
     * @param description
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void registerAsSubscriber(xMsgRegAddress address,
                                     xMsgTopic topic,
                                     String description)
            throws xMsgRegistrationException, IOException {
        register(address.getHost(), address.getPort(), topic, description, false);
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
        register(defaultRegistrarHost, defaultRegistrarPort, topic, description, false);
    }

    /**
     *
     * @param address
     * @param topic
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removePublisherRegistration(xMsgRegAddress address,
                                            xMsgTopic topic)
            throws xMsgRegistrationException, IOException {
        _removeRegistration(address.getHost(), address.getPort(), topic, "", true);
    }

    /**
     *
     * @param topic
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removePublisherRegistration(xMsgTopic topic)
            throws xMsgRegistrationException, IOException {
        _removeRegistration(defaultRegistrarHost, defaultRegistrarPort, topic, "", true);
    }

    /**
     *
     * @param address
     * @param topic
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removeSubscriberRegistration(xMsgRegAddress address,
                                             xMsgTopic topic)
            throws xMsgRegistrationException, IOException {
        _removeRegistration(address.getHost(), address.getPort(), topic, "", false);
    }

    /**
     *
     * @param topic
     * @throws xMsgRegistrationException
     * @throws IOException
     */
    public void removeSubscriberRegistration(xMsgTopic topic)
            throws xMsgRegistrationException, IOException {
        _removeRegistration(defaultRegistrarHost, defaultRegistrarPort, topic, "", false);
    }

    /**
     *
     * @param address
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findPublishers(xMsgRegAddress address,
                                                xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(address.getHost(), address.getPort(), topic, true);
    }

    /**
     *
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findPublishers(xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(defaultRegistrarHost, defaultRegistrarPort, topic, true);
    }

    /**
     *
     * @param address
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findSubscribers(xMsgRegAddress address,
                                                 xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(address.getHost(), address.getPort(), topic, false);
    }

    /**
     *
     * @param topic
     * @return
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findSubscribers(xMsgTopic topic)
            throws xMsgRegistrationException {

        return findRegistration(defaultRegistrarHost, defaultRegistrarPort, topic, false);
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
     *
     * @param topic
     * @param cb
     * @return
     * @throws xMsgException
     */
    public xMsgSubscription subscribe(final xMsgConnection con,
                                      final xMsgTopic topic,
                                      final xMsgCallBack cb)
            throws xMsgException {

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
        regb.setHost(defaultProxyHost);
        regb.setPort(defaultProxyPort);
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

        // create the registration driver object
        xMsgRegDriver regDriver = new xMsgRegDriver(context, regServerIp, regServPort);

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
    private void _removeRegistration(String regServerIp,
                                     int regServPort,
                                     xMsgTopic topic,
                                     String description,
                                     boolean isPublisher)
            throws xMsgRegistrationException {

        // create the registration driver object
        xMsgRegDriver regDriver = new xMsgRegDriver(context, regServerIp, regServPort);

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

        // create the registration driver object
        xMsgRegDriver regDriver = new xMsgRegDriver(context, regServerIp, regServPort);

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
    private xMsgConnection createConnection(xMsgPrxAddress address, xMsgConnectionOption setup) {
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
     * @param con
     * @param msg
     * @param timeout
     * @return
     * @throws xMsgException
     * @throws TimeoutException
     */
    private xMsgMessage _publish(xMsgConnection con, xMsgMessage msg, int timeout)
            throws xMsgException, TimeoutException {

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
            xMsgSubscription sh = subscribe(con, xMsgTopic.wrap(returnAddress), cb);
            cb.setSubscriptionHandler(sh);
        } else {

            // just make sure that receiver knows that this is not a sync request.
            // need this in case we reuse messages.
            msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());
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
            assert cb != null : "xMsg-Error: null callback at sync publish";
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
     * @param mimeType
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    private xMsgMessage _publish(xMsgConnection con,
                                 xMsgTopic topic,
                                 String mimeType, Object data, int timeout)
            throws xMsgException, IOException, TimeoutException {

        // create a message
        xMsgMessage msg;
        if (mimeType != null) {
            msg = new xMsgMessage(topic, mimeType, data);
        } else {
            msg = new xMsgMessage(topic, data);
        }

        return _publish(con, msg, timeout);
    }

    /**
     *
     * Executes a user callback implementing {@link org.jlab.coda.xmsg.core.xMsgCallBack}
     * interface. Note that it is under user responsibility to check metadata "replyTo"
     * to define if this is a sync request and send the result to the topic = replyTo.
     *
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

        // async request
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                callback.callback(callbackMsg);
            }
        });
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
