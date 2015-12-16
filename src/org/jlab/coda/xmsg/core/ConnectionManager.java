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

import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

class ConnectionManager {

    // 0MQ context object
    private final ZContext context;

    // default connection option
    private xMsgConnectionSetup defaultConnectionOption;

    ConnectionManager(ZContext context) {
        this.context = context;

        // default pub/sub socket options
        defaultConnectionOption = new xMsgConnectionSetup() {

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

    xMsgConnection getProxyConnection(xMsgProxyAddress address) {
        return getProxyConnection(address, defaultConnectionOption);
    }

    xMsgConnection getProxyConnection(xMsgProxyAddress address,
                                      xMsgConnectionSetup setup) {
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

    void releaseProxyConnection(xMsgConnection connection) {
        context.destroySocket(connection.getPubSock());
        context.destroySocket(connection.getSubSock());
    }

    xMsgRegDriver getRegistrarConnection(xMsgRegAddress address) {
        return new xMsgRegDriver(context, address);
    }

    void releaseRegistrarConnection(xMsgRegDriver connection) {
        connection.destroy();
    }

    void setLinger(int linger) {
        context.setLinger(linger);
    }

    void destroy() {
        context.destroy();
    }
}
