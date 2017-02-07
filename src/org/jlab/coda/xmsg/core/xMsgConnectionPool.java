package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

import java.io.Closeable;

/**
 * A connection pool that can be shared between actors.
 */
public final class xMsgConnectionPool implements Closeable {

    final ConnectionSetup setup;
    final ConnectionManager connectionManager;

    /**
     * Creates a builder to create a new connection pool.
     *
     * @return a new xMsgConnectionPool builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }


    /**
     * Helps creating and configuring an xMsg connection pool.
     * All parameters not set will be initialized to their default values.
     */
    public static final class Builder extends ConnectionSetup.Builder<Builder> {

        @Override
        Builder getThis() {
            return this;
        }

        /**
         * Creates the connection pool with the defined setup.
         *
         * @return a new connection pool
         */
        public xMsgConnectionPool build() {
            ConnectionSetup setup = new ConnectionSetup(proxyAddress, conSetup.build());
            xMsgConnectionFactory factory = new xMsgConnectionFactory(xMsgContext.getInstance());
            return new xMsgConnectionPool(setup, factory);
        }
    }


    /**
     * Default constructor.
     */
    private xMsgConnectionPool(ConnectionSetup setup,
                               xMsgConnectionFactory factory) {
        this.setup = setup;
        this.connectionManager = new ConnectionManager(factory, setup.connectionSetup());
    }

    /**
     * Closes all connections.
     */
    public void destroy() {
        final int infiniteLinger = -1;
        destroy(infiniteLinger);
    }

    /**
     * Closes all connections.
     *
     * @param linger the linger period when closing the sockets
     * @see <a href="http://api.zeromq.org/3-2:zmq-setsockopt">ZMQ_LINGER</a>
     */
    public void destroy(int linger) {
        connectionManager.destroy(linger);
    }

    /**
     * Closes all connections.
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Obtains a connection to the default proxy.
     * If there is no available connection, a new one will be created.
     *
     * @return a connection to the proxy
     * @throws xMsgException if a new connection could not be created
     */
    public xMsgConnection getConnection() throws xMsgException {
        return getConnection(setup.proxyAddress());
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
        return new xMsgConnection(connectionManager,
                                  connectionManager.getProxyConnection(address));
    }

    /**
     * Destroys the given connection.
     *
     * @param connection the connection to be destroyed
     */
    public void destroyConnection(xMsgConnection connection) {
        connection.destroy();
    }
}
