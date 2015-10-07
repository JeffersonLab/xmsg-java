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
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionOption;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
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
 * xMsg base class that provides methods
 * for organizing pub/sub communications.
 *
 * This class also provides a thread pool for running subscription
 * callbacks in a separate thread.
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsg {

    // the name of this actor
    protected final String myName;

    // thread pool
    private final ThreadPoolExecutor threadPool;

    // default thread pool size
    private int defaultPoolSize;

    // default proxy address
    private xMsgProxyAddress defaultProxyAddr;

    // default registrar address where registrar and
    private xMsgRegAddress defaultRegistrarAddr;

    // 0MQ context object
    private ZContext context = xMsgContext.getContext();

    // default connection option
    private xMsgConnectionOption defaultConnectionOption;

    // map of active subscriptions
    private Map<String, xMsgSubscription> mySubscriptions = new HashMap<>();


    /**
     * Creates an xMsg object using default settings for proxy and registrar.
     * The localhost will be used as a default host for both proxy and registrar.
     * The default proxy port will be set to
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * As a  default port for the registrar will be used
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     *
     * @param name the name of an actor
     * @param poolSize the size of the callback thread pool
     * @throws xMsgAddressException if the IP address of the host could not be resolved
     */
    public xMsg(String name, int poolSize) {
        this(name,
             new xMsgProxyAddress(),
             new xMsgRegAddress(),
             poolSize);
    }

    /**
     * Creates an xMsg object using default settings for proxy and registrar.
     * The localhost will be used as a default host for both proxy and registrar.
     * The default proxy port will be set to
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * As a  default port for the registrar will be used
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * The default pool size {@link org.jlab.coda.xmsg.core.xMsgConstants#DEFAULT_POOL_SIZE}
     * is used to create the xMSg object.
     *
     * @param name the name of an actor
     * @throws xMsgAddressException if the IP address of the host could not be resolved
     */
    public xMsg(String name) {
        this(name,
             new xMsgProxyAddress(),
             new xMsgRegAddress(),
             xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
    }

    /**
     * Creates an xMsg object using default settings for proxy and default port for the registrar.
     * The localhost will be used as a default host for proxy.
     * The default proxy port will be set to
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * As a  default port for the registrar will be used
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     *
     * @param name the name of an actor
     * @param registrarHost the registrar host
     * @param poolSize the size of the callback thread pool
     * @throws xMsgAddressException if the IP address of the host could not be resolved
     */
    public xMsg(String name, String registrarHost, int poolSize) {
        this(name,
             new xMsgProxyAddress(),
             new xMsgRegAddress(registrarHost),
             poolSize);
    }

    /**
     * Creates an xMsg object using default settings for proxy and default port for the registrar.
     * The localhost will be used as a default host for proxy.
     * The default proxy port will be set to
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * As a  default port for the registrar will be used
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * The default pool size {@link org.jlab.coda.xmsg.core.xMsgConstants#DEFAULT_POOL_SIZE}
     * is used to create the xMSg object.
     *
     * @param name the name of an actor
     * @param registrarHost the registrar host
     * @throws xMsgAddressException if the IP address of the host could not be resolved
     */
    public xMsg(String name, String registrarHost) {
        this(name,
             new xMsgProxyAddress(),
             new xMsgRegAddress(registrarHost),
             xMsgConstants.DEFAULT_POOL_SIZE.getIntValue());
    }

    /**
     * Creates an xMsg actor.
     *
     * @param name the name of the actor
     * @param defaultProxyAddr the proxy address
     * @param defaultRegAddr the registrar address
     * @param poolSize the size of the callback thread pool
     * @throws xMsgAddressException if the IP address of the host could not be resolved
     */
    public xMsg(String name,
                xMsgProxyAddress defaultProxyAddr,
                xMsgRegAddress defaultRegAddr,
                int poolSize) {

        // We need to have a name for an actor
        this.myName = name;

        this.defaultPoolSize = poolSize;
        this.defaultProxyAddr = defaultProxyAddr;
        this.defaultRegistrarAddr = defaultRegAddr;

        // create fixed size thread pool
        this.threadPool = xMsgUtil.newFixedThreadPool(defaultPoolSize, name);

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
     * Returns the name of this actor.
     *
     * @return the name of an actor
     */
    public String getName() {
        return myName;
    }


    /**
     * Change the size of the internal thread pool for subscription callbacks.
     */
    public void setPoolSize(int poolSize) {
        threadPool.setCorePoolSize(poolSize);
    }

    /**
     * Returns the size of the callback thread pool.
     *
     * @return the size of the callback thread pool
     */
    public int getPoolSize() {
        return defaultPoolSize;
    }

    /**
     * Makes a connection to a required proxy.
     * This will use the default connection options defined at the constructor.
     * Note that it is reasonable to use this method for connecting to a remote proxy.
     * Yet, this is not a requirement.
     * For local/default proxy connections we suggest using {@link #connect()}
     * or {@link #connect(int port)}
     *
     * @param address {@link org.jlab.coda.xmsg.net.xMsgProxyAddress} object
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public xMsgConnection connect(xMsgProxyAddress address) {
        return createConnection(address, defaultConnectionOption);
    }

    /**
     * Makes a connection to a required proxy.
     * Note that it is reasonable to use this method for connecting to a remote proxy.
     * Yet, this is not a requirement.
     * For local/default proxy connections we suggest using {@link #connect()}
     * or {@link #connect(int port)}
     *
     * @param address {@link org.jlab.coda.xmsg.net.xMsgProxyAddress} object
     * @param setUp {@link org.jlab.coda.xmsg.net.xMsgConnectionOption} object
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public xMsgConnection connect(xMsgProxyAddress address, xMsgConnectionOption setUp) {
        return createConnection(address, setUp);
    }

    /**
     * Makes a connection to the proxy on a specified host.
     * The default proxy port will be used:
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * This will use the default connection options defined at the constructor.
     *
     * @param proxyHost proxy host name
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     * @throws xMsgAddressException if the IP address of the host could not be resolved
     */
    public xMsgConnection connect(String proxyHost) {
        xMsgProxyAddress address = new xMsgProxyAddress(proxyHost);
        return createConnection(address, defaultConnectionOption);
    }

    /**
     * Makes a connection to the default/local proxy. Note that
     * default proxy host and port are defined at the constructor.
     *
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public xMsgConnection connect() {
        return createConnection(defaultProxyAddr, defaultConnectionOption);
    }

    /**
     * Makes a connection to the default/local proxy on a specified port.
     * Note that the default proxy host is defined at the constructor.
     *
     * @param port proxy port number
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public xMsgConnection connect(int port) {
        xMsgProxyAddress address = new xMsgProxyAddress(defaultProxyAddr.host(), port);
        return createConnection(address, defaultConnectionOption);
    }

    /**
     * Disconnects and closes pub and sub  sockets to the proxy.
     *
     * @param connection {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public void release(xMsgConnection connection) {
        context.destroySocket(connection.getPubSock());
        context.destroySocket(connection.getSubSock());
    }

    /**
     * Un-subscribes all previous subscriptions, destroys the
     * 0MQ context and shuts down thread pool.
     *
     * @throws xMsgException
     */
    public void destroy() throws xMsgException {
        for (xMsgSubscription sh : mySubscriptions.values()) {
            unsubscribe(sh);
        }
        context.destroy();
        threadPool.shutdown();
    }

    /**
     * Destructor if this xMsg object.
     *
     * @param linger linger period for closing the 0MQ context
     * @throws xMsgException
     */
    public void destroy(int linger) throws xMsgException {
        context.setLinger(linger);
        destroy();
    }

    /**
     * Registers this actor as a publisher.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the topic to which messages will be published:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the published message
     * @throws xMsgException {@link org.jlab.coda.xmsg.excp.xMsgException}
     */
    public void registerAsPublisher(xMsgRegAddress address,
                                    xMsgTopic topic,
                                    String description)
            throws xMsgException {
        register(address, topic, description, true);
    }

    /**
     * Registers an actor as a publisher. This assumes that the
     * request is addressed to the default registrar.
     *
     * @param topic the topic to which messages will be published:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the published message
     * @throws xMsgException {@link org.jlab.coda.xmsg.excp.xMsgException}
     */
    public void registerAsPublisher(xMsgTopic topic,
                                    String description)
            throws xMsgException {
        register(defaultRegistrarAddr, topic, description, true);
    }

    /**
     * Registers this actor as a subscriber.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the subscription
     * @throws xMsgException {@link org.jlab.coda.xmsg.excp.xMsgException}
     */
    public void registerAsSubscriber(xMsgRegAddress address,
                                     xMsgTopic topic,
                                     String description)
            throws xMsgException {
        register(address, topic, description, false);
    }

    /**
     * Registers an actor as a subscriber. This assumes that the
     * request is addressed to the default registrar.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the subscription
     * @throws xMsgException {@link org.jlab.coda.xmsg.excp.xMsgException}
     */
    public void registerAsSubscriber(xMsgTopic topic,
                                     String description)
            throws xMsgException {
        register(defaultRegistrarAddr, topic, description, false);
    }

    /**
     * Sends a request to the registrar to remove a publisher registration.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @throws xMsgException
     */
    public void removePublisherRegistration(xMsgRegAddress address,
                                            xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(address, topic, "", true);
    }

    /**
     * Sends a request to the registrar to remove a publisher registration.
     * This assumes that the request is addressed to the default registrar.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @throws xMsgException
     */
    public void removePublisherRegistration(xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(defaultRegistrarAddr, topic, "", true);
    }

    /**
     * Sends a request to the registrar to remove a subscriber registration.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @throws xMsgException
     */
    public void removeSubscriberRegistration(xMsgRegAddress address,
                                             xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(address, topic, "", false);
    }

    /**
     * Sends a request to the registrar to remove a subscriber registration.
     * This assumes that the request is addressed to the default registrar.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @throws xMsgException
     */
    public void removeSubscriberRegistration(xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(defaultRegistrarAddr, topic, "", false);
    }

    /**
     * Sends a request to the registrar to find/return a publisher registration.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return Set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgException
     */
    public Set<xMsgRegistration> findPublishers(xMsgRegAddress address,
                                                xMsgTopic topic)
            throws xMsgException {

        return findRegistration(address, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a publisher registration.
     * This assumes that the request is addressed to the default registrar.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return Set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgException
     */
    public Set<xMsgRegistration> findPublishers(xMsgTopic topic)
            throws xMsgException {

        return findRegistration(defaultRegistrarAddr, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a subscriber registration.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return Set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgException
     */
    public Set<xMsgRegistration> findSubscribers(xMsgRegAddress address,
                                                 xMsgTopic topic)
            throws xMsgException {

        return findRegistration(address, topic, false);
    }

    /**
     * Sends a request to the registrar to find/return a subscriber registration.
     * This assumes that the request is addressed to the default registrar.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @return Set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgException
     */
    public Set<xMsgRegistration> findSubscribers(xMsgTopic topic)
            throws xMsgException {

        return findRegistration(defaultRegistrarAddr, topic, false);
    }

    /**
     * Publishes a message through the specified proxy connection.
     *
     * @param con a proxy connection:
     *            object of {@link org.jlab.coda.xmsg.net.xMsgConnection}
     * @param msg publishing message:
     *            object of {@link org.jlab.coda.xmsg.core.xMsgMessage}
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
     * Publishes a message through the specified proxy connection and blocks waiting
     * for a response from a subscriber. Note that the subscriber must check "replyTo"
     * metadata field and publish the response to that address.
     *
     * @param con a proxy connection:
     *            object of {@link org.jlab.coda.xmsg.net.xMsgConnection}
     * @param msg publishing message:
     *            object of {@link org.jlab.coda.xmsg.core.xMsgMessage}
     * @param timeout int that defines how long publisher will wait for
     *                subscribers response in milli seconds.
     * @return {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage syncPublish(xMsgConnection con,
                                   xMsgMessage msg,
                                   int timeout) throws xMsgException, TimeoutException {
        return _publish(con, msg, timeout);
    }

    /**
     * Subscribes to a specific topic. New subscription handlers will be stored
     * in a local map of subscriptions. This method will throw an exception
     * in case requested subscription already exists in the map of subscriptions.
     *
     * @param con a proxy connection:
     *            object of {@link org.jlab.coda.xmsg.net.xMsgConnection}
     * @param topic subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param cb {@link org.jlab.coda.xmsg.core.xMsgCallBack} object
     * @return {@link org.jlab.coda.xmsg.core.xMsgSubscription} object.
     * @throws xMsgException
     */
    public xMsgSubscription subscribe(final xMsgConnection con,
                                      final xMsgTopic topic,
                                      final xMsgCallBack cb)
            throws xMsgException {

        // define a unique name for the subscription
        String name = "sub-" + myName + "-" + con.getAddress() + "-" + topic;

        // check to see if the subscription already exists
        if (mySubscriptions.containsKey(name)) {
            throw new xMsgException("xMsg-Warning:  Subscription exists.");
        }

        // get sub socket
        Socket sock = con.getSubSock();
        if (sock == null) {
            throw new xMsgException("xMsg-Error: null sub socket");
        }

        xMsgSubscription sHandle = new xMsgSubscription(name, con, topic) {
            @Override
            public void handle(ZMsg inputMsg) throws xMsgException, IOException {
                final xMsgMessage callbackMsg = new xMsgMessage(inputMsg);
                callUserCallBack(cb, callbackMsg);
            }
        };

        sHandle.start();

        // add the new subscription to the subscriptions map
        mySubscriptions.put(name, sHandle);

        return sHandle;
    }

    /**
     * Stops the existing subscription.
     *
     * @param handle SubscribeHandler object reference:
     *               object of {@link org.jlab.coda.xmsg.core.xMsgSubscription}
     * @throws xMsgException
     */
    public void unsubscribe(xMsgSubscription handle)
            throws xMsgException {
        handle.stop();
        mySubscriptions.remove(handle.getName());
    }

    // ..............................................................//
    //                        Private section
    // ..............................................................//

    /**
     * Creates xMsgRegistration builder object. This is a proto-buffer
     * definition of actor registration data.
     *
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the actor
     * @return {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder} Object
     */
    private Builder createRegistration(xMsgTopic topic, String description) {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(myName);
        regb.setHost(defaultProxyAddr.host());
        regb.setPort(defaultProxyAddr.port());
        regb.setDomain(topic.domain());
        regb.setSubject(topic.subject());
        regb.setType(topic.type());
        regb.setDescription(description);
        return regb;
    }

    /**
     * Registers an actor with the registrar, running at the specified host and port.
     *
     * @param regServerIp the host name of the registrar
     * @param regServPort the port of the registrar
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the actor
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private void register(xMsgRegAddress regAddress,
                          xMsgTopic topic,
                          String description,
                          boolean isPublisher)
            throws xMsgException {
        xMsgRegDriver regDriver = new xMsgRegDriver(context, regAddress);

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
     * Removes an actor from the registrar database, assuming that the
     * registrar is running on the specified host and port.
     *
     * @param regServerIp the host name of the registrar
     * @param regServPort the port of the registrar
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the actor
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private void _removeRegistration(xMsgRegAddress regAddress,
                                     xMsgTopic topic,
                                     String description,
                                     boolean isPublisher)
            throws xMsgException {

        // create the registration driver object
        xMsgRegDriver regDriver = new xMsgRegDriver(context, regAddress);

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
     * Finds the registration information of an actor, previously registered
     * with a registrar running on the specified host and port.
     *
     * @param regServerIp the host name of the registrar
     * @param regServPort the port of the registrar
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private Set<xMsgRegistration> findRegistration(xMsgRegAddress regAddress,
                                                   xMsgTopic topic,
                                                   boolean isPublisher)
            throws xMsgException {

        // create the registration driver object
        xMsgRegDriver regDriver = new xMsgRegDriver(context, regAddress);

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
     * Creates two 0MQ tcp socket connections to a proxy defined by the xMsgPrAddress.
     *
     * @param address the proxy address:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgProxyAddress}
     * @param setup {@link org.jlab.coda.xmsg.net.xMsgConnectionOption} object
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    private xMsgConnection createConnection(xMsgProxyAddress address, xMsgConnectionOption setup) {
        Socket pubSock = context.createSocket(ZMQ.PUB);
        Socket subSock = context.createSocket(ZMQ.SUB);
        setup.preConnection(pubSock);
        setup.preConnection(subSock);

        int pubPort = address.port();
        int subPort = pubPort + 1;
        pubSock.connect("tcp://" + address.host() + ":" + pubPort);
        subSock.connect("tcp://" + address.host() + ":" + subPort);
        setup.postConnection();

        xMsgConnection connection = new xMsgConnection();
        connection.setAddress(address);
        connection.setPubSock(pubSock);
        connection.setSubSock(subSock);

        return connection;
    }

    /**
     * Publishes a message through a defined proxy connection. In this case
     * {@link org.jlab.coda.xmsg.net.xMsgConnection} object is used. The timeout
     * parameter defines if the publishing is going to sync or async.
     *
     * @param con proxy connection {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     * @param msg {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     * @param timeout timeout > 0 defines publishing process as a sync process
     * @return in case of the sync operation we expect an object of
     * {@link org.jlab.coda.xmsg.core.xMsgMessage} from a receiver.
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
            while (cb.recvMsg == null && t < timeout) {
                t++;
                xMsgUtil.sleep(1);
            }
            if (t >= timeout) {
                throw new TimeoutException("xMsg-Error: no response for time_out = " + t + " milli sec.");
            }
            msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());
            return cb.recvMsg;
        }
        return null;
    }

    /**
     * Executes a user callback implementing {@link org.jlab.coda.xmsg.core.xMsgCallBack}
     * interface. Note that it is under user responsibility to check metadata "replyTo"
     * to define if this is a sync request and send the result to the topic = replyTo.
     *
     * @param callback the actual user callback implementing
     *                 the interface {@link org.jlab.coda.xmsg.core.xMsgCallBack}
     * @param callbackMsg the message  {@link org.jlab.coda.xmsg.core.xMsgMessage}
     *                    passed to the user callback
     * @throws xMsgException
     * @throws IOException
     */
    private void callUserCallBack(final xMsgCallBack callback,
                                  final xMsgMessage callbackMsg)
            throws xMsgException, IOException {

        // async request
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    callback.callback(callbackMsg);
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
