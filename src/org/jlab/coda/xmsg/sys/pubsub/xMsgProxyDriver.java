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

package org.jlab.coda.xmsg.sys.pubsub;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

public abstract class xMsgProxyDriver {

    protected final xMsgProxyAddress address;
    protected final Socket socket;

    private final xMsgSocketFactory factory;


    public static xMsgProxyDriver publisher(xMsgProxyAddress address, xMsgSocketFactory factory)
            throws xMsgException {
        return new Pub(address, factory);
    }

    public static xMsgProxyDriver subscriber(xMsgProxyAddress address, xMsgSocketFactory factory)
            throws xMsgException {
        return new Sub(address, factory);
    }


    private xMsgProxyDriver(int type, xMsgProxyAddress address, xMsgSocketFactory factory)
            throws xMsgException {
        this.address = address;
        this.socket = factory.createSocket(type);
        this.factory = factory;
    }

    public void connect() throws xMsgException {
        factory.connectSocket(socket, address.host(), getPort());
    }

    abstract int getPort();

    public boolean checkConnection(long timeout) throws xMsgException {
        String identity = IdentityGenerator.getCtrlId();
        Socket ctrlSocket = createControlSocket(identity);
        try (Poller poller = factory.context().poller(1)) {
            poller.register(ctrlSocket, Poller.POLLIN);

            long pollTimeout = timeout < 100 ? timeout : 100;
            long totalTime = 0;
            while (totalTime < timeout) {
                try {
                    ZMsg ctrlMsg = new ZMsg();
                    ctrlMsg.add(xMsgCtrlConstants.CTRL_TOPIC + ":con");
                    ctrlMsg.add(xMsgCtrlConstants.CTRL_CONNECT);
                    ctrlMsg.add(identity);
                    ctrlMsg.send(getSocket());

                    poller.poll(pollTimeout);
                    if (poller.pollin(0)) {
                        ZMsg replyMsg = ZMsg.recvMsg(ctrlSocket);
                        if (replyMsg.size() == 1) {
                            ZFrame typeFrame = replyMsg.pop();
                            String type = new String(typeFrame.getData());
                            if (type.equals(xMsgCtrlConstants.CTRL_CONNECT)) {
                                return true;
                            }
                        }
                    }
                    totalTime += pollTimeout;
                } catch (ZMQException e) {
                    e.printStackTrace();
                }
            }
            return false;
        } finally {
            factory.closeQuietly(ctrlSocket);
        }
    }

    public void subscribe(String topic) {
        socket.subscribe(topic.getBytes());
    }

    public boolean checkSubscription(String topic, long timeout) throws xMsgException {
        Socket pubSocket = createPubSocket();
        try (Poller poller = factory.context().poller(1)) {
            poller.register(getSocket(), Poller.POLLIN);

            long pollTimeout = timeout < 100 ? timeout : 100;
            long totalTime = 0;
            while (totalTime < timeout) {
                try {
                    ZMsg ctrlMsg = new ZMsg();
                    ctrlMsg.add(xMsgCtrlConstants.CTRL_TOPIC + ":sub");
                    ctrlMsg.add(xMsgCtrlConstants.CTRL_SUBSCRIBE);
                    ctrlMsg.add(topic);
                    ctrlMsg.send(pubSocket);

                    poller.poll(pollTimeout);
                    if (poller.pollin(0)) {
                        ZMsg replyMsg = ZMsg.recvMsg(getSocket());
                        if (replyMsg.size() == 2) {
                            ZFrame idFrame = replyMsg.pop();
                            ZFrame typeFrame = replyMsg.pop();

                            String id = new String(idFrame.getData());
                            String type = new String(typeFrame.getData());
                            if (id.equals(topic) && type.equals(xMsgCtrlConstants.CTRL_SUBSCRIBE)) {
                                return true;
                            }
                        }
                    }
                    totalTime += pollTimeout;
                } catch (ZMQException e) {
                    e.printStackTrace();
                }
            }
            return false;
        } finally {
            factory.closeQuietly(pubSocket);
        }
    }

    public void unsubscribe(String topic) {
        socket.unsubscribe(topic.getBytes());
    }

    public void send(ZMsg msg) {
        msg.send(socket);
    }

    public ZMsg recv() {
        return ZMsg.recvMsg(socket);
    }

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


    static class Pub extends xMsgProxyDriver {

        Pub(xMsgProxyAddress address, xMsgSocketFactory factory) throws xMsgException {
            super(ZMQ.PUB, address, factory);
        }

        @Override
        int getPort() {
            return address.pubPort();
        }

        @Override
        public boolean checkSubscription(String topic, long timeout) {
            throw new UnsupportedOperationException("PUB socket cannot subscribe");
        }
    }


    static class Sub extends xMsgProxyDriver {

        Sub(xMsgProxyAddress address, xMsgSocketFactory factory) throws xMsgException {
            super(ZMQ.SUB, address, factory);
        }

        @Override
        int getPort() {
            return address.subPort();
        }

        @Override
        public boolean checkConnection(long timeout) {
            return true;
        }
    }


    private Socket createControlSocket(String identity) throws xMsgException {
        Socket socket = factory.createSocket(ZMQ.DEALER);
        try {
            socket.setIdentity(identity.getBytes());
            factory.connectSocket(socket, address.host(), address.pubPort() + 2);
            return socket;
        } catch (Exception e) {
            factory.closeQuietly(socket);
            throw e;
        }
    }

    private Socket createPubSocket() throws xMsgException {
        Socket socket = factory.createSocket(ZMQ.PUB);
        try {
            factory.connectSocket(socket, address.host(), address.pubPort());
            return socket;
        } catch (Exception e) {
            factory.closeQuietly(socket);
            throw e;
        }
    }
}
