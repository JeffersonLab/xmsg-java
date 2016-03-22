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
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import zmq.ZError;

public class xMsgSocketFactory {

    private final ZContext ctx;

    public xMsgSocketFactory(ZContext ctx) {
        this.ctx = ctx;
    }

    public Socket createSocket(int type) throws xMsgException {
        try {
            Socket socket = ctx.createSocket(type);
            socket.setRcvHWM(0);
            socket.setSndHWM(0);
            return socket;
        } catch (IllegalStateException e) {
            throw new xMsgException("Reached maximum number of sockets");
        }
    }

    public void bindSocket(Socket socket, int port) throws xMsgException {
        try {
            socket.bind("tcp://*:" + port);
        } catch (ZMQException e) {
            if (e.getErrorCode() == ZMQ.Error.EADDRINUSE.getCode()) {
                throw new xMsgException("Could not bind to port " + port);
            }
            throw e;
        }
    }

    public void connectSocket(Socket socket, String host, int port) throws xMsgException {
        try {
            socket.connect("tcp://" + host + ":" + port);
        } catch (ZMQException e) {
            if (e.getErrorCode() == ZMQ.Error.EMTHREAD.getCode()) {
                throw new xMsgException("No I/O thread available", e);
            }
        }
    }

    public void setLinger(Socket socket, int linger) {
        try {
            socket.setLinger(linger);
        } catch (ZError.CtxTerminatedException e) {
            // ignore
        }
    }

    public void destroySocket(Socket socket) {
        ctx.destroySocket(socket);
    }
}
