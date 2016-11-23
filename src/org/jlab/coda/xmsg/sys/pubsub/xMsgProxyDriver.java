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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

public abstract class xMsgProxyDriver {

    protected final xMsgProxyAddress address;
    protected final String identity;
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
        this.identity = IdentityGenerator.getCtrlId();
        this.socket = factory.createSocket(type);
        this.factory = factory;
    }

    public void connect() throws xMsgException {
        factory.connectSocket(socket, address.host(), getPort());
    }

    abstract int getPort();

    public boolean checkConnection(long timeout) {
        Socket ctrlSocket;
        try {
            ctrlSocket = factory.createSocket(ZMQ.DEALER);
            ctrlSocket.setIdentity(identity.getBytes());
            factory.connectSocket(ctrlSocket, address.host(), address.pubPort() + 2);
        } catch (Exception e) {
            return false;
        }

        Poller items = new Poller(1);
        items.register(ctrlSocket, Poller.POLLIN);

        long pollTimeout = timeout < 100 ? timeout : 100;
        long totalTime = 0;
        while (totalTime < timeout) {
            try {
                ZMsg ctrlMsg = new ZMsg();
                ctrlMsg.add(xMsgConstants.CTRL_TOPIC + ":con");
                ctrlMsg.add(xMsgConstants.CTRL_CONNECT);
                ctrlMsg.add(identity);
                ctrlMsg.send(getSocket());

                items.poll(pollTimeout);
                if (items.pollin(0)) {
                    ZMsg replyMsg = ZMsg.recvMsg(ctrlSocket);
                    try {
                        if (replyMsg.size() == 1) {
                            ZFrame typeFrame = replyMsg.pop();
                            try {
                                String type = new String(typeFrame.getData());
                                if (type.equals(xMsgConstants.CTRL_CONNECT)) {
                                    factory.closeQuietly(ctrlSocket);
                                    return true;
                                }
                            } finally {
                                typeFrame.destroy();
                            }
                        }
                    } finally {
                        replyMsg.destroy();
                    }
                }
                totalTime += pollTimeout;
            } catch (ZMQException e) {
                e.printStackTrace();
            }
        }
        factory.closeQuietly(ctrlSocket);
        return false;
    }

    public void subscribe(String topic) {
        socket.subscribe(topic.getBytes());
    }

    public boolean checkSubscription(String topic, long timeout) {
        Socket pubSocket;
        try {
            pubSocket = factory.createSocket(ZMQ.PUB);
            factory.connectSocket(pubSocket, address.host(), address.pubPort());
        } catch (Exception e) {
            return false;
        }

        Poller items = new Poller(1);
        items.register(getSocket(), Poller.POLLIN);

        long pollTimeout = timeout < 100 ? timeout : 100;
        long totalTime = 0;
        while (totalTime < timeout) {
            try {
                ZMsg ctrlMsg = new ZMsg();
                ctrlMsg.add(xMsgConstants.CTRL_TOPIC + ":sub");
                ctrlMsg.add(xMsgConstants.CTRL_SUBSCRIBE);
                ctrlMsg.add(topic);
                ctrlMsg.send(pubSocket);

                items.poll(pollTimeout);
                if (items.pollin(0)) {
                    ZMsg replyMsg = ZMsg.recvMsg(getSocket());
                    try {
                        if (replyMsg.size() == 2) {
                            ZFrame idFrame = replyMsg.pop();
                            ZFrame typeFrame = replyMsg.pop();
                            try {
                                String id = new String(idFrame.getData());
                                String type = new String(typeFrame.getData());
                                if (id.equals(topic) && type.equals(xMsgConstants.CTRL_SUBSCRIBE)) {
                                    factory.closeQuietly(pubSocket);
                                    return true;
                                }
                            } finally {
                                idFrame.destroy();
                                typeFrame.destroy();
                            }
                        }
                    } finally {
                        replyMsg.destroy();
                    }
                }
                totalTime += pollTimeout;
            } catch (ZMQException e) {
                e.printStackTrace();
            }
        }
        factory.closeQuietly(pubSocket);
        return false;
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

    public String getIdentity() {
        return identity;
    }

    public Socket getSocket() {
        return socket;
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
}
