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

import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;

class ConnectionManager {

    // Factory
    private final xMsgConnectionFactory factory;

    // pool of proxy connections
    private final ConnectionPool<xMsgProxyAddress, xMsgConnection> proxyConnections;

    // pool of registrar connections
    private final ConnectionPool<xMsgRegAddress, xMsgRegDriver> registrarConnections;

    // default connection option
    private xMsgConnectionSetup defaultConnectionOption;

    ConnectionManager(ZContext context) {
        this(new xMsgConnectionFactory(context));
    }

    ConnectionManager(xMsgConnectionFactory factory) {
        this.factory = factory;
        this.proxyConnections = new ConnectionPool<>();
        this.registrarConnections = new ConnectionPool<>();

        // default pub/sub socket options
        defaultConnectionOption = new xMsgConnectionSetup() { };
    }

    xMsgConnection createProxyConnection(xMsgProxyAddress address) {
        return createProxyConnection(address, defaultConnectionOption);
    }

    xMsgConnection createProxyConnection(xMsgProxyAddress address,
                                      xMsgConnectionSetup setup) {
        return factory.createProxyConnection(address, setup);
    }

    xMsgConnection getProxyConnection(xMsgProxyAddress address) {
        return getProxyConnection(address, defaultConnectionOption);
    }

    xMsgConnection getProxyConnection(xMsgProxyAddress address,
                                      xMsgConnectionSetup setup) {
        xMsgConnection cachedConnection = proxyConnections.getConnection(address);
        if (cachedConnection != null) {
            return cachedConnection;
        }
        return factory.createProxyConnection(address, setup);
    }

    void releaseProxyConnection(xMsgConnection connection) {
        proxyConnections.setConnection(connection.getAddress(), connection);
    }

    void destroyProxyConnection(xMsgConnection connection) {
        factory.destroyProxyConnection(connection);
    }

    xMsgRegDriver getRegistrarConnection(xMsgRegAddress address) {
        xMsgRegDriver cachedConnection = registrarConnections.getConnection(address);
        if (cachedConnection != null) {
            return cachedConnection;
        }
        return factory.createRegistrarConnection(address);
    }

    void releaseRegistrarConnection(xMsgRegDriver connection) {
        registrarConnections.setConnection(connection.getAddress(), connection);
    }

    void setDefaultConnectionSetup(xMsgConnectionSetup setup) {
        defaultConnectionOption = setup;
    }

    void setLinger(int linger) {
        factory.setLinger(linger);
    }

    void destroy() {
        proxyConnections.destroyAll(c -> factory.destroyProxyConnection(c));
        registrarConnections.destroyAll(c -> factory.destroyRegistrarConnection(c));
        factory.destroy();
    }


    static class ConnectionPool<A, C> {
        private Map<A, Queue<C>> connections = new ConcurrentHashMap<>();

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
                cache.getValue().forEach(destroy::accept);
            }
        }
    }
}
