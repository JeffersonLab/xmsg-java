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

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgConnectionSetup;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;

class ConnectionManager {

    // Factory
    private final xMsgConnectionFactory factory;

    // pool of proxy connections
    private final ConnectionPool<xMsgProxyAddress, xMsgProxyDriver> proxyConnections;

    // pool of registrar connections
    private final ConnectionPool<xMsgRegAddress, xMsgRegDriver> registrarConnections;

    // default connection option
    private volatile xMsgConnectionSetup proxySetup;

    ConnectionManager(xMsgConnectionFactory factory) {
        this(factory, xMsgConnectionSetup.newBuilder().build());
    }

    ConnectionManager(xMsgConnectionFactory factory, xMsgConnectionSetup setup) {
        this.factory = factory;
        this.proxyConnections = new ConnectionPool<>();
        this.registrarConnections = new ConnectionPool<>();
        this.proxySetup = setup;
    }

    xMsgProxyDriver createProxySubscriber(xMsgProxyAddress address) throws xMsgException {
        return factory.createSubscriberConnection(address, proxySetup);
    }

    xMsgProxyDriver createProxyConnection(xMsgProxyAddress address) throws xMsgException {
        return factory.createPublisherConnection(address, proxySetup);
    }

    xMsgProxyDriver getProxyConnection(xMsgProxyAddress address) throws xMsgException {
        xMsgProxyDriver cachedConnection = proxyConnections.getConnection(address);
        if (cachedConnection != null) {
            return cachedConnection;
        }
        return createProxyConnection(address);
    }

    void releaseProxyConnection(xMsgProxyDriver connection) {
        proxyConnections.setConnection(connection.getAddress(), connection);
    }

    xMsgRegDriver getRegistrarConnection(xMsgRegAddress address) throws xMsgException {
        xMsgRegDriver cachedConnection = registrarConnections.getConnection(address);
        if (cachedConnection != null) {
            return cachedConnection;
        }
        return factory.createRegistrarConnection(address);
    }

    void releaseRegistrarConnection(xMsgRegDriver connection) {
        registrarConnections.setConnection(connection.getAddress(), connection);
    }

    void destroy(int linger) {
        proxyConnections.destroyAll(c -> c.close(linger));
        registrarConnections.destroyAll(c -> c.close());
    }


    static class ConnectionPool<A, C> {
        private final Map<A, Queue<C>> connections = new ConcurrentHashMap<>();

        public C getConnection(A address) {
            Queue<C> cache = connections.get(address);
            if (cache != null) {
                return cache.poll();
            }
            return null;
        }

        public void setConnection(A address, C connection) {
            Queue<C> cache = connections.get(address);
            if (cache == null) {
                cache = new ConcurrentLinkedQueue<>();
                Queue<C> tempCache = connections.putIfAbsent(address, cache);
                if (tempCache != null) {
                    cache = tempCache;
                }
            }
            cache.add(connection);
        }

        public void destroyAll(Consumer<C> destroy) {
            for (Map.Entry<A, Queue<C>> cache : connections.entrySet()) {
                cache.getValue().forEach(destroy);
            }
        }
    }
}
