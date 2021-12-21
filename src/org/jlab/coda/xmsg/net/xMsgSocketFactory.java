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

package org.jlab.coda.xmsg.net;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import zmq.ZError;

/**
 * Creates and connects new 0MQ sockets.
 */
public class xMsgSocketFactory {

    private final Context ctx;

    /**
     * Creates a new socket factory.
     *
     * @param ctx the ZMQ context to be used by sockets
     */
    public xMsgSocketFactory(Context ctx) {
        this.ctx = ctx;
    }

    /**
     * Creates a new 0MQ socket.
     *
     * @param type the type of the socket
     * @return a new socket of the given type
     * @throws xMsgException if the context cannot create more sockets
     */
    public Socket createSocket(int type) throws xMsgException {
        try {
            Socket socket = ctx.socket(type);
            socket.setRcvHWM(0);
            socket.setSndHWM(0);
            return socket;
        } catch (IllegalStateException e) {
            int maxSockets = xMsgContext.getInstance().getMaxSockets();
            throw new xMsgException("reached maximum number of sockets: " + maxSockets);
        }
    }

    /**
     * Binds the given socket to the given port.
     *
     * @param socket the socket to bind
     * @param port the listening port
     * @throws xMsgException if the port is in use
     */
    public void bindSocket(Socket socket, int port) throws xMsgException {
        try {
            socket.bind("tcp://*:" + port);
        } catch (ZMQException e) {
            if (e.getErrorCode() == ZMQ.Error.EADDRINUSE.getCode()) {
                throw new xMsgException("could not bind to port " + port);
            }
            throw e;
        }
    }

    /**
     * Connects the given socket to the given port.
     *
     * @param socket the socket to be connected
     * @param host the address of the host
     * @param port the connection port
     * @throws xMsgException if no 0MQ I/O threads are available for the connection
     */
    public void connectSocket(Socket socket, String host, int port) throws xMsgException {
        try {
            socket.connect("tcp://" + host + ":" + port);
        } catch (ZMQException e) {
            if (e.getErrorCode() == ZMQ.Error.EMTHREAD.getCode()) {
                throw new xMsgException("no I/O thread available", e);
            }
        }
    }

    /**
     * Sets the linger period for the given socket.
     *
     * @param socket the socket to be configured
     * @param linger the linger period, in milliseconds
     */
    public void setLinger(Socket socket, int linger) {
        try {
            socket.setLinger(linger);
        } catch (ZError.CtxTerminatedException e) {
            // ignore
        }
    }

    /**
     * Closes the given socket.
     *
     * @param socket the socket to be closed, it can be null
     */
    public void closeQuietly(Socket socket) {
        if (socket != null) {
            socket.close();
        }
    }

    /**
     * Returns the ZMQ context used by this factory.
     *
     * @return the context
     */
    public Context context() {
        return ctx;
    }
}
