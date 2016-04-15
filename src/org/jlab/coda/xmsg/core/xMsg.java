/*
 *    Copyright (C) 2016. Jefferson Lab (JLAB). All Rights Reserved.
 *    Permission to use, copy, modify, and distribute this software and its
 *    documentation for governmental use, educational, research, and not-for-profit
 *    purposes, without fee and without a signed licensing agreement.
 *
 *    IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 *    INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 *    THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 *    OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *    JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *    THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *    PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 *    HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 *    SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *    This software was developed under the United States Government License.
 *    For more information contact author at gurjyan@jlab.org
 *    Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZMQException;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The main xMsg pub/sub actor.
 * <p>
 * Actors send messages to each other using pub/sub communications
 * through a cloud of xMsg proxies.
 * Registrar services provide registration and discoverability of actors.
 * <p>
 * An actor has a <em>name</em> for identification, a <em>default proxy</em> intended for
 * long-term publication/subscription of messages, and a <em>default registrar</em>
 * where it can register and discover other long-term actors.
 * Unless otherwise specified, the local node and the standard ports will be
 * used for both default proxy and registrar.
 * <p>
 * Publishers set specific <em>topics</em> for their messages, and subscribers define
 * topics of interest to filter which messages they want to receive.
 * A <em>domain-specific callback</em> defined by the subscriber will be executed every
 * time a message is received. This callback must be thread-safe,
 * and it can also be used to send responses or new messages.
 * <p>
 * In order to publish or subscribe to messages, a <em>connection</em> to a proxy must
 * be obtained. The actor owns and keeps a <em>pool of available connections</em>,
 * creating new ones as needed. When no address is specified, the <em>default
 * proxy</em> will be used. The connections can be returned to the pool of available
 * connections, to avoid creating too many new connections. All connections will
 * be closed when the actor is destroyed.
 * <p>
 * Multi-threaded publication of messages is fully supported, but every thread
 * must use its own connection. Subscriptions of messages always run in their
 * own background thread. It is recommended to always obtain and release the
 * necessary connections inside the thread that uses them. The <em>connect</em> methods
 * will ensure that each thread gets a different connection.
 * <p>
 * Publishers must be sending messages through the same <em>proxy</em> than the
 * subscribers for the messages to be received. Normally, this proxy will be
 * the <em>default proxy</em> of a long-term subscriber with many dynamic publishers, or
 * the <em>default proxy</em> of a long-term publisher with many dynamic subscribers.
 * To have many publishers sending messages to many subscribers, they all must
 * <em>agree</em> in the proxy. It is possible to use several proxies, but multiple
 * publications and subscriptions will be needed, and it may get complicated.
 * Applications using xMsg have great flexibility to organize their
 * communications, but it is better to deploy simple topologies.
 * <p>
 * Actors can register as publishers and/or subscribers with <em>registrar services</em>,
 * so other actors can discover them if they share the topic of interest.
 * Using the registration and discovery methods is always thread-safe.
 * The registrar service must be common to the actors, running in a known node.
 * If no address is specified, the <em>default registrar</em> will be used.
 * Note that the registration will always set the <em>default proxy</em> as the proxy
 * through which the actor is publishing/subscribed to messages.
 * If registration for different proxies is needed, multiple actors should be
 * used, each one with an appropriate default proxy.
 * <p>
 * The proxy and the registrar are provided as stand-alone executables,
 * but only the Java implementation can be used to run a registrar.
 *
 * @see xMsgMessage
 * @see xMsgTopic
 */
public class xMsg implements AutoCloseable {

    /** The identifier of this actor. */
    protected final String myName;

    /** The generated unique ID of this actor. */
    protected final String myId;

    // thread pool
    private final ThreadPoolExecutor threadPool;

    // default proxy address
    private final xMsgProxyAddress defaultProxyAddress;

    // default registrar address where registrar and
    private final xMsgRegAddress defaultRegistrarAddress;

    private final ConnectionManager connectionManager;

    // map of active subscriptions
    private final ConcurrentMap<String, xMsgSubscription> mySubscriptions;

    private ResponseListener syncPubListener;

    /**
     * Creates an actor with default settings.
     * The local node and the standard ports will be used for both
     * default proxy and registrar, and the callback thread-pool will use the
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#DEFAULT_POOL_SIZE default pool size}.
     *
     * @param name the name of this actor
     * @see xMsgProxyAddress
     * @see xMsgRegAddress
     */
    public xMsg(String name) {
        this(name,
             new xMsgProxyAddress(),
             new xMsgRegAddress(),
             xMsgConstants.DEFAULT_POOL_SIZE);
    }

    /**
     * Creates an actor with default settings.
     * The local node and the standard ports will be used for both
     * default proxy and registrar.
     *
     * @param name the name of this actor
     * @param poolSize the initial size of the callback thread-pool
     * @see xMsgProxyAddress
     * @see xMsgRegAddress
     */
    public xMsg(String name, int poolSize) {
        this(name,
             new xMsgProxyAddress(),
             new xMsgRegAddress(),
             poolSize);
    }

    /**
     * Creates an actor specifying the default registrar to be used.
     * The local node and the standard ports will be used for the default proxy,
     * and the callback thread-pool will use the
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#DEFAULT_POOL_SIZE default pool size}.
     *
     * @param name the name of an actor
     * @param defaultRegistrar the address to the default registrar
     */
    public xMsg(String name, xMsgRegAddress defaultRegistrar) {
        this(name,
             new xMsgProxyAddress(),
             defaultRegistrar,
             xMsgConstants.DEFAULT_POOL_SIZE);
    }

    /**
     * Creates an actor specifying the default registrar to be used.
     * The local node and the standard ports will be used for the default proxy.
     *
     * @param name the name of this actor
     * @param defaultRegistrar the address to the default registrar
     * @param poolSize the initial size of the callback thread-pool
     * @see xMsgProxyAddress
     */
    public xMsg(String name, xMsgRegAddress defaultRegistrar, int poolSize) {
        this(name,
             new xMsgProxyAddress(),
             defaultRegistrar,
             poolSize);
    }

    /**
     * Creates an actor specifying the default proxy and registrar to be used.
     *
     * @param name the name of this actor
     * @param defaultProxy the address to the default proxy
     * @param defaultRegistrar the address to the default registrar
     * @param poolSize the size of the callback thread pool
     */
    public xMsg(String name,
                xMsgProxyAddress defaultProxy,
                xMsgRegAddress defaultRegistrar,
                int poolSize) {
        this(name,
             defaultProxy,
             defaultRegistrar,
             new xMsgConnectionFactory(xMsgContext.getContext()),
             poolSize);
    }

    /**
     * Full constructor.
     */
    protected xMsg(String name,
                   xMsgProxyAddress defaultProxy,
                   xMsgRegAddress defaultRegistrar,
                   xMsgConnectionFactory factory,
                   int poolSize) {
        // We need to have a name for an actor
        this.myName = name;
        this.myId = xMsgUtil.encodeIdentity(defaultRegistrar.toString(), name);

        this.defaultProxyAddress = defaultProxy;
        this.defaultRegistrarAddress = defaultRegistrar;

        // create fixed size thread pool
        this.threadPool = xMsgUtil.newFixedThreadPool(poolSize, name);

        // create the connection pool
        this.connectionManager = new ConnectionManager(factory);

        // create the responses listener
        this.syncPubListener = new ResponseListener(myId, factory);
        this.syncPubListener.start();

        // create the map of running subscriptions
        this.mySubscriptions = new ConcurrentHashMap<>();
    }

    /**
     * Unsubscribes all previous subscriptions,
     * shuts down thread pool and closes all connections.
     */
    public void destroy() {
        final int infiniteLinger = -1;
        destroy(infiniteLinger);
    }

    /**
     * Unsubscribes all previous subscriptions,
     * shuts down thread pool and closes all connections.
     *
     * @param linger linger period for closing the 0MQ context
     */
    public void destroy(int linger) {
        for (xMsgSubscription sh : mySubscriptions.values()) {
            unsubscribe(sh);
        }
        syncPubListener.stop();
        try {
            threadPool.shutdownNow();
            threadPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Interrupted when shutting down subscription thread-pool.");
        }
        connectionManager.destroy(linger);
    }

    /**
     * Unsubscribes all previous subscriptions,
     * shuts down thread pool and closes all connections.
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Returns the name of this actor.
     */
    public String getName() {
        return myName;
    }

    /**
     * Changes the size of the callback thread-pool.
     */
    public void setPoolSize(int poolSize) {
        threadPool.setCorePoolSize(poolSize);
    }

    /**
     * Returns the size of the callback thread pool.
     */
    public int getPoolSize() {
        return threadPool.getMaximumPoolSize();
    }

    /**
     * Returns the address of the default proxy used by this actor.
     */
    public xMsgProxyAddress getDefaultProxyAddress() {
        return defaultProxyAddress;
    }

    /**
     * Returns the address of the default registrar used by this actor.
     */
    public xMsgRegAddress getDefaultRegistrarAddress() {
        return defaultRegistrarAddress;
    }

    /**
     * Overwrites the default setup for all created connection.
     * This setup will be applied every time a new connection is created.
     *
     * @param setup the new default setup
     */
    public void setConnectionSetup(xMsgConnectionSetup setup) {
        connectionManager.setDefaultConnectionSetup(setup);
    }

    /**
     * Creates a new connection to the specified proxy.
     *
     * @param address the address of the proxy
     * @return a connection to the proxy
     * @throws xMsgException if the connection could not be created
     */
    public xMsgConnection createConnection(xMsgProxyAddress address) throws xMsgException {
        return connectionManager.createProxyConnection(address);
    }

    /**
     * Creates a new connection to the specified proxy host and
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#DEFAULT_PORT default port}.
     *
     * @param proxyHost the host name of the proxy
     * @return a connection to the proxy
     * @throws xMsgException if the connection could not be created
     */
    public xMsgConnection createConnection(String proxyHost) throws xMsgException {
        xMsgProxyAddress address = new xMsgProxyAddress(proxyHost);
        return connectionManager.createProxyConnection(address);
    }

    /**
     * Creates a new connection to the default proxy.
     *
     * @return a connection to the proxy
     * @throws xMsgException if the connection could not be created
     */
    public xMsgConnection createConnection() throws xMsgException {
        return connectionManager.createProxyConnection(defaultProxyAddress);
    }

    /**
     * Obtains a connection to the specified proxy.
     * If there is no available connection, a new one will be created.
     *
     * @param address the address of the proxy
     * @return a connection to the proxy
     * @throws xMsgException if a new connection could not be created
     */
    public xMsgConnection getConnection(xMsgProxyAddress address) throws xMsgException {
        return connectionManager.getProxyConnection(address);
    }

    /**
     * Obtains a connection to the specified proxy host and
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#DEFAULT_PORT default port}.
     * If there is no available connection, a new one will be created.
     *
     * @param proxyHost the host name of the proxy
     * @return a connection to the proxy
     * @throws xMsgException if a new connection could not be created
     */
    public xMsgConnection getConnection(String proxyHost) throws xMsgException {
        xMsgProxyAddress address = new xMsgProxyAddress(proxyHost);
        return connectionManager.getProxyConnection(address);
    }

    /**
     * Obtains a connection to the default proxy.
     * If there is no available connection, a new one will be created.
     *
     * @return a connection to the proxy
     * @throws xMsgException if a new connection could not be created
     */
    public xMsgConnection getConnection() throws xMsgException {
        return connectionManager.getProxyConnection(defaultProxyAddress);
    }

    /**
     * Returns the given connection into the pool of available connections.
     *
     * @param connection the returned connection
     */
    public void releaseConnection(xMsgConnection connection) {
        connectionManager.releaseProxyConnection(connection);
    }

    /**
     * Destroys the given connection.
     *
     * @param connection the connection to be destroyed
     */
    public void destroyConnection(xMsgConnection connection) {
        connection.close();
    }

    /**
     * Registers this actor as a publisher of the specified topic,
     * on the given registrar service.
     *
     * The actor will be registered as publishing through the default proxy.
     *
     * @param address the address of the registrar service
     * @param topic the topic to which messages will be published
     * @param description general description of the published messages
     * @throws xMsgException if the registration failed
     */
    public void registerAsPublisher(xMsgRegAddress address,
                                    xMsgTopic topic,
                                    String description)
            throws xMsgException {
        _register(address, topic, description, true);
    }

    /**
     * Registers this actor as a publisher of the specified topic,
     * on the default registrar service.
     *
     * The actor will be registered as publishing through the default proxy.
     *
     * @param topic the topic to which messages will be published
     * @param description general description of the published messages
     * @throws xMsgException if the registration failed
     */
    public void registerAsPublisher(xMsgTopic topic,
                                    String description)
            throws xMsgException {
        _register(defaultRegistrarAddress, topic, description, true);
    }

    /**
     * Registers this actor as a subscriber of the specified topic,
     * on the given registrar service.
     *
     * The actor will be registered as subscribed through the default proxy.
     *
     * @param address the address of the registrar service
     * @param topic the topic of the subscription
     * @param description general description of the subscription
     * @throws xMsgException if the registration failed
     */
    public void registerAsSubscriber(xMsgRegAddress address,
                                     xMsgTopic topic,
                                     String description)
            throws xMsgException {
        _register(address, topic, description, false);
    }

    /**
     * Registers this actor as a subscriber of the specified topic,
     * on the default registrar service.
     *
     * The actor will be registered as subscribed through the default proxy.
     *
     * @param topic the topic of the subscription
     * @param description general description of the subscription
     * @throws xMsgException if the registration failed
     */
    public void registerAsSubscriber(xMsgTopic topic,
                                     String description)
            throws xMsgException {
        _register(defaultRegistrarAddress, topic, description, false);
    }

    /**
     * Removes this actor as a publisher of the specified topic,
     * from the given registrar service.
     *
     * @param address the address of the registrar service
     * @param topic the topic to which messages are published
     * @throws xMsgException if the request failed
     */
    public void deregisterAsPublisher(xMsgRegAddress address,
                                      xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(address, topic, true);
    }

    /**
     * Removes this actor as a publisher of the specified topic,
     * from the default registrar service.
     *
     * @param topic the topic to which messages are published
     * @throws xMsgException if the request failed
     */
    public void deregisterAsPublisher(xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(defaultRegistrarAddress, topic, true);
    }

    /**
     * Removes this actor as a subscriber of the specified topic,
     * from the given registrar service.
     *
     * @param address the address of the registrar service
     * @param topic the topic of the subscription
     * @throws xMsgException if the request failed
     */
    public void deregisterAsSubscriber(xMsgRegAddress address,
                                       xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(address, topic, false);
    }

    /**
     * Removes this actor as a subscriber from the given registrar.
     * The default registrar is defined at the constructor.
     *
     * @param topic the subscription topic
     * @throws xMsgException if the request failed
     */
    public void deregisterAsSubscriber(xMsgTopic topic)
            throws xMsgException {
        _removeRegistration(defaultRegistrarAddress, topic, false);
    }


    /**
     * Finds all publishers of the specified topic
     * that are registered on the given registrar service.
     *
     * @param address the address to the registrar service
     * @param topic the topic of interest
     * @return a set with the registration data of the matching publishers
     * @throws xMsgException if the request failed
     */
    public Set<xMsgRegistration> findPublishers(xMsgRegAddress address, xMsgTopic topic)
            throws xMsgException {
        return _findRegistration(address, topic, true);
    }

    /**
     * Finds all publishers of the specified topic
     * that are registered on the default registrar service.
     *
     * @param topic the topic of interest
     * @return a set with the registration data of the matching publishers
     * @throws xMsgException if the request failed
     */
    public Set<xMsgRegistration> findPublishers(xMsgTopic topic)
            throws xMsgException {
        return _findRegistration(defaultRegistrarAddress, topic, true);
    }

    /**
     * Finds all subscribers to the specified topic
     * that are registered on the given registrar service.
     *
     * @param address the address to the registrar service
     * @param topic the topic of interest
     * @return a set with the registration data of the matching subscribers
     * @throws xMsgException if the request failed
     */
    public Set<xMsgRegistration> findSubscribers(xMsgRegAddress address, xMsgTopic topic)
            throws xMsgException {
        return _findRegistration(address, topic, false);
    }

    /**
     * Finds all subscribers to the specified topic
     * that are registered on the default registrar service.
     *
     * @param topic the topic of interest
     * @return a set with the registration data of the matching subscribers
     * @throws xMsgException if the request failed
     */
    public Set<xMsgRegistration> findSubscribers(xMsgTopic topic)
            throws xMsgException {
        return _findRegistration(defaultRegistrarAddress, topic, false);
    }

    /**
     * Publishes a message through the specified proxy connection.
     *
     * @param connection the connection to the proxy
     * @param msg the message to be published
     * @throws xMsgException if the request failed
     */
    public void publish(xMsgConnection connection, xMsgMessage msg) throws xMsgException {
        // just make sure that receiver knows that this is not a sync request.
        // need this in case we reuse messages.
        msg.getMetaData().clearReplyTo();

        _publish(connection, msg);
    }

    /**
     * Publishes a message through the specified proxy connection and blocks
     * waiting for a response.
     *
     * The subscriber must publish the response to the topic given by the
     * {@code replyto} metadata field, through the same proxy.
     *
     * This method will throw if a response is not received before the timeout
     * expires.
     *
     * @param connection the connection to the proxy
     * @param msg the message to be published
     * @param timeout the length of time to wait a response, in milliseconds
     * @return the response message
     * @throws xMsgException
     * @throws TimeoutException
     */
    public xMsgMessage syncPublish(xMsgConnection connection,
                                   xMsgMessage msg,
                                   int timeout) throws xMsgException, TimeoutException {
        // address/topic where the subscriber should send the result
        String returnAddress = xMsgUtil.getUniqueReplyTo(myId);

        // set the return address as replyTo in the xMsgMessage
        msg.getMetaData().setReplyTo(returnAddress);

        // subscribe to the returnAddress
        syncPubListener.register(connection.getAddress());

        try {
            // it must be the internal _publish, to keep the replyTo field
            _publish(connection, msg);

            // wait for the response
            return syncPubListener.waitMessage(returnAddress, timeout);
        } finally {
            msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.toString());
        }
    }

    /**
     * Subscribes to a topic of interest through the specified proxy
     * connection.
     * A background thread will be started to receive the messages.
     *
     * @param connection the connection to the proxy
     * @param topic the topic to select messages
     * @param callback the user action to run when a message is received
     * @throws xMsgException
     */
    public xMsgSubscription subscribe(xMsgConnection connection,
                                      xMsgTopic topic,
                                      xMsgCallBack callback) throws xMsgException {
        // define a unique name for the subscription
        String name = "sub-" + myName + "-" + connection.getAddress() + "-" + topic;

        xMsgSubscription sHandle = mySubscriptions.get(name);
        if (sHandle == null) {
            sHandle = new xMsgSubscription(name, connection, topic) {
                @Override
                public void handle(xMsgMessage inputMsg) throws xMsgException {
                    threadPool.submit(() -> callback.callback(inputMsg));
                }
            };
            xMsgSubscription result = mySubscriptions.putIfAbsent(name, sHandle);
            if (result == null) {
                sHandle.start();
                return sHandle;
            }
        }
        throw new IllegalStateException("subscription already exists");
    }

    /**
     * Stops the given subscription.
     *
     * @param handle an active subscription
     * @throws xMsgException
     */
    public void unsubscribe(xMsgSubscription handle) {
        handle.stop();
        mySubscriptions.remove(handle.getName());
    }

    // ..............................................................//
    //                        Private section
    // ..............................................................//

    private xMsgRegistration.Builder _createRegistration(xMsgTopic topic, boolean isPublisher) {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(myName);
        regb.setHost(defaultProxyAddress.host());
        regb.setPort(defaultProxyAddress.pubPort());
        regb.setDomain(topic.domain());
        regb.setSubject(topic.subject());
        regb.setType(topic.type());
        if (isPublisher) {
            regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        } else {
            regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        }
        return regb;
    }

    private void _register(xMsgRegAddress regAddress,
                           xMsgTopic topic,
                           String description,
                           boolean isPublisher) throws xMsgException {
        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        try {
            xMsgRegistration.Builder regb = _createRegistration(topic, isPublisher);
            regb.setDescription(description);
            regDriver.register(regb.build(), isPublisher);
            connectionManager.releaseRegistrarConnection(regDriver);
        } catch (ZMQException | xMsgException e) {
            regDriver.close();
            throw e;
        }
    }

    private void _removeRegistration(xMsgRegAddress regAddress,
                                     xMsgTopic topic,
                                     boolean isPublisher) throws xMsgException {
        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        try {
            xMsgRegistration.Builder regb = _createRegistration(topic, isPublisher);
            regDriver.removeRegistration(regb.build(), isPublisher);
            connectionManager.releaseRegistrarConnection(regDriver);
        } catch (ZMQException | xMsgException e) {
            regDriver.close();
            throw e;
        }
    }

    private Set<xMsgRegistration> _findRegistration(xMsgRegAddress regAddress,
                                                    xMsgTopic topic,
                                                    boolean isPublisher) throws xMsgException {
        xMsgRegDriver regDriver = connectionManager.getRegistrarConnection(regAddress);
        try {
            xMsgRegistration.Builder regb = _createRegistration(topic, isPublisher);
            Set<xMsgRegistration> result = regDriver.findRegistration(regb.build(), isPublisher);
            connectionManager.releaseRegistrarConnection(regDriver);
            return result;
        } catch (ZMQException | xMsgException e) {
            regDriver.close();
            throw e;
        }
    }

    private void _publish(xMsgConnection connection, xMsgMessage msg) throws xMsgException {
        try {
            connection.send(msg.serialize());
        } catch (ZMQException e) {
            throw new xMsgException("Publishing failed: " + e.getMessage());
        }
    }
}
