/*
 *    Copyright (C) 2021. Jefferson Lab (JLAB). All Rights Reserved.
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
import org.jlab.coda.xmsg.sys.p2p.xMsgPtpDriver;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.io.Closeable;

/**
 * The point-to-point connection to xMsg nodes.
 * The actor must be destroyed in order to close this connections.
 * <p>
 */
public class xMsgConnectionPtp implements Closeable {

    private xMsgPtpDriver connection;

    xMsgConnectionPtp(xMsgPtpDriver connection) {
        this.connection = connection;
    }

    /**
     * Use {@link xMsg#destroyConnection} to destroy the connection and actually
     * close the socket.
     */
    @Override
    public void close() {
        connection = null;
    }

    void destroy() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }


    void push(byte[] data) throws xMsgException {
        if (connection == null) {
            throw new IllegalStateException("connection is closed");
        }
        try {
            connection.send(data);
        } catch (ZMQException e) {
            destroy();
            throw new xMsgException("could not push message", e);
        }
    }

    void push(xMsgMessagePtp msg) throws xMsgException {
        if (connection == null) {
            throw new IllegalStateException("connection is closed");
        }
        try {
            connection.send(msg.getData());
        } catch (ZMQException e) {
            destroy();
            throw new xMsgException("could not push message", e);
        }
    }


    byte[] pull() throws xMsgException {
        if (connection == null) {
            throw new IllegalStateException("connection is closed");
        }

        try {
            return connection.recvData();
        } catch (ZMQException e) {
            destroy();
            throw new xMsgException("could not pull data", e);
        }
    }

    xMsgMessagePtp pullMsg() throws xMsgException {
        if (connection == null) {
            throw new IllegalStateException("connection is closed");
        }

        try {
            return new xMsgMessagePtp(connection.recv());
        } catch (ZMQException e) {
            destroy();
            throw new xMsgException("could not pull message", e);
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
