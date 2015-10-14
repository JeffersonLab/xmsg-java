/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionOption;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
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
    private xMsgProxyAddress defaultProxyAddress;

    // default registrar address where registrar and
    private xMsgRegAddress defaultRegistrarAddress;

    private ConnectionManager connectionManager;

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
     * @param defaultProxyAddress the proxy address
     * @param defaultRegAddr the registrar address
     * @param poolSize the size of the callback thread pool
     */
    public xMsg(String name,
                xMsgProxyAddress defaultProxyAddress,
                xMsgRegAddress defaultRegAddr,
                int poolSize) {

        // We need to have a name for an actor
        this.myName = name;

        this.defaultPoolSize = poolSize;
        this.defaultProxyAddress = defaultProxyAddress;
        this.defaultRegistrarAddress = defaultRegAddr;

        // create fixed size thread pool
        this.threadPool = xMsgUtil.newFixedThreadPool(defaultPoolSize, name);

        // create the connection pool
        this.connectionManager = new ConnectionManager(xMsgContext.getContext());
    }

    /**
     * Constructor for testing purposes.
     */
    xMsg(String name, int poolSize, ConnectionManager connectionManager) {

        // We need to have a name for an actor
        this.myName = name;

        this.defaultPoolSize = poolSize;
        this.defaultProxyAddress = new xMsgProxyAddress();
        this.defaultRegistrarAddress = new xMsgRegAddress();

        // create fixed size thread pool
        this.threadPool = xMsgUtil.newFixedThreadPool(defaultPoolSize, name);

        // create the connection pool
        this.connectionManager = connectionManager;
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

    public xMsgProxyAddress getDefaultProxyAddress() {
        return defaultProxyAddress;
    }

    public xMsgRegAddress getDefaultRegistrarAddress() {
        return defaultRegistrarAddress;
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
        return connectionManager.getProxyConnection(address);
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
        return connectionManager.getProxyConnection(address, setUp);
    }

    /**
     * Makes a connection to the proxy on a specified host.
     * The default proxy port will be used:
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * This will use the default connection options defined at the constructor.
     *
     * @param proxyHost proxy host name
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public xMsgConnection connect(String proxyHost) {
        xMsgProxyAddress address = new xMsgProxyAddress(proxyHost);
        return connectionManager.getProxyConnection(address);
    }

    /**
     * Makes a connection to the default/local proxy. Note that
     * default proxy host and port are defined at the constructor.
     *
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public xMsgConnection connect() {
        return connectionManager.getProxyConnection(defaultProxyAddress);
    }

    /**
     * Makes a connection to the default/local proxy on a specified port.
     * Note that the default proxy host is defined at the constructor.
     *
     * @param port proxy port number
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public xMsgConnection connect(int port) {
        xMsgProxyAddress address = new xMsgProxyAddress(defaultProxyAddress.host(), port);
        return connectionManager.getProxyConnection(address);
    }

    /**
     * Disconnects and closes pub and sub  sockets to the proxy.
     *
     * @param connection {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     */
    public void release(xMsgConnection connection) {
        connectionManager.releaseProxyConnection(connection);
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
        connectionManager.destroy();
        threadPool.shutdown();
    }

    /**
     * Destructor if this xMsg object.
     *
     * @param linger linger period for closing the 0MQ context
     * @throws xMsgException
     */
    public void destroy(int linger) throws xMsgException {
        connectionManager.setLinger(linger);
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
        _register(address, topic, description, true);
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
        _register(defaultRegistrarAddress, topic, description, true);
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
        _register(address, topic, description, false);
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
        _register(defaultRegistrarAddress, topic, description, false);
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
        _removeRegistration(address, topic, true);
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
        _removeRegistration(defaultRegistrarAddress, topic, true);
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
        _removeRegistration(address, topic, false);
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
        _removeRegistration(defaultRegistrarAddress, topic, false);
    }


    /**
     * Sends a request to the registrar to find/return a registered publisher
     * domain names.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     *              Topic in this case is ignored.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findPublisherDomainNames(xMsgRegAddress address,
                                                xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredDomainNames(address, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a registered publisher
     * domain names.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     *              Topic in this case is ignored.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findPublisherDomainNames(xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredDomainNames(defaultRegistrarAddress, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a registered subscriber
     * domain names.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     *              Topic in this case is ignored.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findSubscriberDomainNames(xMsgRegAddress address,
                                                xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredDomainNames(address, topic, false);
    }

    /**
     * Sends a request to the registrar to find/return a registered subscriber
     * domain names.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     *              Topic in this case is ignored.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findSubscriberDomainNames(xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredDomainNames(defaultRegistrarAddress, topic, false);
    }

    /**
     * Sends a request to the registrar to find/return a registered publisher
     * subject names in a specific domain. Domain is specified within the
     * topic parameter.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findPublisherSubjectNames(xMsgRegAddress address,
                                           xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredSubjectNames(address, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a registered publisher
     * subject names in a specific domain. Domain is specified within the
     * topic parameter.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findPublisherSubjectNames(xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredSubjectNames(defaultRegistrarAddress, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a registered subscriber
     * subject names in a specific domain. Domain is specified within the
     * topic parameter.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findSubscriberSubjectNames(xMsgRegAddress address,
                                           xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredSubjectNames(address, topic, false);
    }

    /**
     * Sends a request to the registrar to find/return a registered subscriber
     * subject names in a specific domain. Domain is specified within the
     * topic parameter.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findSubscriberSubjectNames(xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredSubjectNames(defaultRegistrarAddress, topic, false);
    }

    /**
     * Sends a request to the registrar to find/return a registered publisher
     * type names in a specific domain and subject. Domain and subject are specified
     * within the topic parameter.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findPublisherTypeNames(xMsgRegAddress address,
                                            xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredTypeNames(address, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a registered publisher
     * type names in a specific domain and subject. Domain and subject are specified
     * within the topic parameter.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findPublisherTypeNames(xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredTypeNames(defaultRegistrarAddress, topic, true);
    }

    /**
     * Sends a request to the registrar to find/return a registered subscriber
     * type names in a specific domain and subject. Domain and subject are specified
     * within the topic parameter.
     *
     * @param address the address of the registrar:
     *                object of {@link org.jlab.coda.xmsg.net.xMsgRegAddress}
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findSubscriberTypeNames(xMsgRegAddress address,
                                            xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredTypeNames(address, topic, false);
    }

    /**
     * Sends a request to the registrar to find/return a registered subscriber
     * type names in a specific domain and subject. Domain and subject are specified
     * within the topic parameter.
     *
     * @param topic the subscription topic:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}.
     * @return space separated names in a single String
     * @throws xMsgException
     */
    public String findSubscriberTypeNames(xMsgTopic topic)
            throws xMsgException {

        return _findRegisteredTypeNames(defaultRegistrarAddress, topic, false);
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

        return _findRegistration(address, topic, true);
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

        return _findRegistration(defaultRegistrarAddress, topic, true);
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

        return _findRegistration(address, topic, false);
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

        return _findRegistration(defaultRegistrarAddress, topic, false);
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
                _callUserCallBack(cb, callbackMsg);
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
     * @return {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder} Object
     */
    private xMsgRegistration.Builder _createRegistration(xMsgTopic topic) {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(myName);
        regb.setHost(defaultProxyAddress.host());
        regb.setPort(defaultProxyAddress.port());
        regb.setDomain(topic.domain());
        regb.setSubject(topic.subject());
        regb.setType(topic.type());
        return regb;
    }

    /**
     * Registers an actor with the registrar, running at the specified host and port.
     *
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param description textual description of the actor
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private void _register(xMsgRegAddress regAddress,
                           xMsgTopic topic,
                           String description,
                           boolean isPublisher)
            throws xMsgException {
        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);

        xMsgRegistration.Builder regb = _createRegistration(topic);
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        regb.setDescription(description);
        xMsgRegistration regData = regb.build();
        regDriver.register(regData, isPublisher);
    }

    /**
     * Removes an actor from the registrar database, assuming that the
     * registrar is running on the specified host and port.
     *
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private void _removeRegistration(xMsgRegAddress regAddress,
                                     xMsgTopic topic,
                                     boolean isPublisher)
            throws xMsgException {

        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        xMsgRegistration.Builder regb = _createRegistration(topic);
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
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private Set<xMsgRegistration> _findRegistration(xMsgRegAddress regAddress,
                                                    xMsgTopic topic,
                                                    boolean isPublisher)
            throws xMsgException {

        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        xMsgRegistration.Builder regb = _createRegistration(topic);
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        xMsgRegistration regData = regb.build();
        return regDriver.findRegistration(regData, isPublisher);
    }

    /**
     * Sends a request to search the database for registered
     * domain names.
     *
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private String _findRegisteredDomainNames(xMsgRegAddress regAddress,
                                                    xMsgTopic topic,
                                                    boolean isPublisher)
            throws xMsgException {

        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        xMsgRegistration.Builder regb = _createRegistration(topic);
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        xMsgRegistration regData = regb.build();
        return regDriver.findRegisteredDomainNames(regData, isPublisher);
    }

    /**
     * Sends a request to search the database for registered
     * subject names for a defined (as part of the topic parameter) domain.
     *
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private String _findRegisteredSubjectNames(xMsgRegAddress regAddress,
                                                    xMsgTopic topic,
                                                    boolean isPublisher)
            throws xMsgException {

        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        xMsgRegistration.Builder regb = _createRegistration(topic);
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        xMsgRegistration regData = regb.build();
        return regDriver.findRegisteredSubjectNames(regData, isPublisher);
    }

    /**
     * Sends a request to search the database for registered
     * subject names for a defined (as part of the topic parameter)
     * domain and subject.
     *
     * @param topic actor's topic of interest:
     *              object of {@link org.jlab.coda.xmsg.core.xMsgTopic}
     * @param isPublisher boolean defines the type of the actor:
     *                    true = publisher, false = subscriber
     * @throws xMsgException
     */
    private String _findRegisteredTypeNames(xMsgRegAddress regAddress,
                                                    xMsgTopic topic,
                                                    boolean isPublisher)
            throws xMsgException {

        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        xMsgRegistration.Builder regb = _createRegistration(topic);
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        xMsgRegistration regData = regb.build();
        return regDriver.findRegisteredTypeNames(regData, isPublisher);
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
            try {
                if (t >= timeout) {
                    throw new TimeoutException("xMsg-Error: no response for time_out = " + t + " milli sec.");
                }
                return cb.recvMsg;
            } finally {
                msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());
                unsubscribe(cb.handler);
            }
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
    private void _callUserCallBack(final xMsgCallBack callback,
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
            return recvMsg;
        }
    }
}
