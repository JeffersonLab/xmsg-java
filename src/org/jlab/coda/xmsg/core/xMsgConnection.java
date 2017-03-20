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

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.zeromq.ZMQException;

import java.io.Closeable;

/**
 * The standard connection to xMsg nodes.
 * <p>
 * Connections are managed by the xMsg actor, that keeps an internal connection
 * pool to cache and reuse connections.
 * The actor must be destroyed in order to close all connections cached in the
 * connection pool.
 * An external pool can also be used to shared connections between many actors.
 * <p>
 * Connections should be closed in order to return them to the connection pool,
 * so they can be reused by other publishing threads.
 */
public class xMsgConnection implements Closeable {

    private final ConnectionManager pool;
    private xMsgProxyDriver connection;

    xMsgConnection(ConnectionManager pool, xMsgProxyDriver connection) {
        this.pool = pool;
        this.connection = connection;
    }

    /**
     * If not destroyed, returns this connection the connection pool to be
     * reused.
     * Use {@link xMsg#destroyConnection} to destroy the connection and actually
     * close the socket.
     */
    @Override
    public void close() {
        if (connection != null) {
            pool.releaseProxyConnection(connection);
            connection = null;
        }
    }

    void destroy() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    void publish(xMsgMessage msg) throws xMsgException {
        if (connection == null) {
            throw new IllegalStateException("connection is closed");
        }
        try {
            connection.send(msg.serialize());
        } catch (ZMQException e) {
            destroy();
            throw new xMsgException("could not publish message", e);
        }
    }

    /**
     * Returns the address of the connected proxy.
     *
     * @return the address of the proxy
     */
    public xMsgProxyAddress getAddress() {
        if (connection == null) {
            throw new IllegalStateException("connection is closed");
        }
        return connection.getAddress();
    }
}
