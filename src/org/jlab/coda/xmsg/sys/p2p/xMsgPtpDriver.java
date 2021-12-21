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

package org.jlab.coda.xmsg.sys.p2p;

import org.jlab.coda.xmsg.core.xMsgMessagePtp;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

/**
 * This class is a simplified form of pubsub.xMsgProxyDriver.
 */
public abstract class xMsgPtpDriver {

    protected final xMsgProxyAddress address;
    protected final Socket socket;

    private final xMsgSocketFactory factory;


    public static xMsgPtpDriver pusher(xMsgProxyAddress address, xMsgSocketFactory factory)
            throws xMsgException {
        return new Push(address, factory);
    }

    public static xMsgPtpDriver puller(xMsgProxyAddress address, xMsgSocketFactory factory)
            throws xMsgException {
        return new Pull(address, factory);
    }


    private xMsgPtpDriver(int type, xMsgProxyAddress address, xMsgSocketFactory factory)
            throws xMsgException {
        this.address = address;
        this.socket = factory.createSocket(type);
        this.factory = factory;
    }

    public void connect() throws xMsgException {
        factory.connectSocket(socket, address.host(), getPort());
    }

    public void bind() throws xMsgException {
        factory.bindSocket(socket, getPort());
    }


    abstract int getPort();



    public void send(byte[] data) { socket.send(data); }

    public void send(xMsgMessagePtp msg) { socket.send(msg.getData()); }

    public ZMsg recv() { return ZMsg.recvMsg(socket); }

    public byte[] recvData() { return socket.recv(); }



    public void close() {
        factory.closeQuietly(socket);
    }

    public void close(int linger) {
        factory.setLinger(socket, linger);
        factory.closeQuietly(socket);
    }

    public xMsgProxyAddress getAddress() {
        return address;
    }

    public Socket getSocket() {
        return socket;
    }

    public Context getContext() {
        return factory.context();
    }


    static class Push extends xMsgPtpDriver {

        Push(xMsgProxyAddress address, xMsgSocketFactory factory) throws xMsgException {
            super(ZMQ.PUSH, address, factory);
        }

        @Override
        int getPort() {
            return address.pubPort();
        }
    }


    static class Pull extends xMsgPtpDriver {

        Pull(xMsgProxyAddress address, xMsgSocketFactory factory) throws xMsgException {
            super(ZMQ.PULL, address, factory);
        }

        @Override
        int getPort() { return address.pubPort(); }
    }

}
