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

import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

class ConnectionManager {

    // Factory
    private final xMsgConnectionFactory factory;

    // default connection option
    private xMsgConnectionSetup defaultConnectionOption;

    ConnectionManager(ZContext context) {
        this(new xMsgConnectionFactory(context));
    }

    ConnectionManager(xMsgConnectionFactory factory) {
        this.factory = factory;
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
    }

    xMsgConnection getProxyConnection(xMsgAddress address) {
        return getProxyConnection(address, defaultConnectionOption);
    }

    xMsgConnection getProxyConnection(xMsgAddress address,
                                      xMsgConnectionSetup setup) {
        return factory.createProxyConnection(address, setup);
    }

    void releaseProxyConnection(xMsgConnection connection) {
        factory.destroyProxyConnection(connection);
    }

    void setDefaultConnectionSetup(xMsgConnectionSetup setup) {
        defaultConnectionOption = setup;
    }

    void setLinger(int linger) {
        factory.setLinger(linger);
    }

    void destroy() {
        factory.destroy();
    }
}
