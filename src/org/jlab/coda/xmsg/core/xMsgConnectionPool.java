/*
 *    Copyright (C) 2017. Jefferson Lab (JLAB). All Rights Reserved.
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

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;

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
     * <p>
     * Creating new connections takes some time, and the first published
     * messages may be lost. The {@link #cacheConnection()} method can be used
     * to create connections before using them .
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
     * <p>
     * Creating new connections takes some time, and the first published
     * messages may be lost. The {@link #cacheConnection(xMsgProxyAddress)}
     * method can be used to create connections before using them .
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
     * Creates and stores a connection to the default proxy.
     * Useful to ensure that there is a connection ready when {@link
     * #getConnection()} is called.
     *
     * @throws xMsgException if the new connection could not be created
     */
    public void cacheConnection() throws xMsgException {
        cacheConnection(setup.proxyAddress());
    }

    /**
     * Creates and stores a connection to the specified proxy.
     * Useful to ensure that there is a connection ready when {@link
     * #getConnection(xMsgProxyAddress)} is called.
     *
     * @param address the address of the proxy
     * @throws xMsgException if the new connection could not be created
     */
    public void cacheConnection(xMsgProxyAddress address) throws xMsgException {
        xMsgProxyDriver driver = connectionManager.createProxyConnection(address);
        connectionManager.releaseProxyConnection(driver);
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
