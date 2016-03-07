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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Random;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

public class xMsgConnectionFactory {

    private final ZContext context;

    public xMsgConnectionFactory(ZContext context) {
        this.context = context;

        // fix default linger
        this.context.setLinger(-1);
    }

    public xMsgConnection createProxyConnection(xMsgProxyAddress address,
                                                xMsgConnectionSetup setup) throws xMsgException {

        Socket pubSock = createSocket(ZMQ.PUB);
        Socket subSock = createSocket(ZMQ.SUB);
        Socket ctrlSock = createSocket(ZMQ.DEALER);

        String identity = getCtrlId();
        ctrlSock.setIdentity(identity.getBytes());

        setup.preConnection(pubSock);
        setup.preConnection(subSock);

        int pubPort = address.port();
        int subPort = pubPort + 1;
        int ctrlPort = subPort + 1;

        connectSocket(pubSock, address.host(), pubPort);
        connectSocket(subSock, address.host(), subPort);
        connectSocket(ctrlSock, address.host(), ctrlPort);

        if (!checkConnection(pubSock, ctrlSock, identity)) {
            context.destroySocket(pubSock);
            context.destroySocket(subSock);
            context.destroySocket(ctrlSock);
            throw new xMsgException("Could not connect to " + address);
        }
        setup.postConnection();

        xMsgConnection connection = new xMsgConnection();
        connection.setAddress(address);
        connection.setPubSock(pubSock);
        connection.setSubSock(subSock);
        connection.setControlSock(ctrlSock);
        connection.setIdentity(identity);

        return connection;
    }

    public xMsgRegDriver createRegistrarConnection(xMsgRegAddress address) throws xMsgException {
        Socket socket = createSocket(ZMQ.REQ);
        socket.setHWM(0);
        connectSocket(socket, address.host(), address.port());
        return new xMsgRegDriver(address, socket);
    }

    public void destroyProxyConnection(xMsgConnection connection) {
        context.destroySocket(connection.getPubSock());
        context.destroySocket(connection.getSubSock());
    }

    public void destroyRegistrarConnection(xMsgRegDriver connection) {
        context.destroySocket(connection.getSocket());
    }

    public void setLinger(int linger) {
        context.setLinger(linger);
    }

    public void destroy() {
        context.destroy();
    }


    private Socket createSocket(int type) throws xMsgException {
        try {
            return context.createSocket(type);
        } catch (IllegalStateException e) {
            throw new xMsgException("Reached maximum number of sockets");
        }
    }

    private void connectSocket(Socket socket, String host, int port) throws xMsgException {
        try {
            socket.connect("tcp://" + host + ":" + port);
        } catch (ZMQException e) {
            if (e.getErrorCode() == ZMQ.Error.EMTHREAD.getCode()) {
                throw new xMsgException("No I/O thread available", e);
            }
        }
    }

    private boolean checkConnection(Socket pubSocket, Socket ctrlSocket, String identity) {
        ZMQ.Poller items = new ZMQ.Poller(1);
        items.register(ctrlSocket, ZMQ.Poller.POLLIN);
        int retry = 0;
        while (retry < 10) {
            retry++;
            ZMsg ctrlMsg = new ZMsg();
            try {
                ctrlMsg.add(xMsgConstants.CTRL_TOPIC);
                ctrlMsg.add(xMsgConstants.CTRL_CONNECT);
                ctrlMsg.add(identity);
                ctrlMsg.send(pubSocket);

                items.poll(100);
                if (items.pollin(0)) {
                    ZMsg replyMsg = ZMsg.recvMsg(ctrlSocket);
                    try {
                        // TODO: check the message
                        return true;
                    } finally {
                        replyMsg.destroy();
                    }
                }
            } catch (ZMQException e) {
                e.printStackTrace();
            } finally {
                ctrlMsg.destroy();
            }
        }
        return false;
    }


    // CHECKSTYLE.OFF: ConstantName
    private static final Random randomGenerator = new Random();
    private static final long ctrlIdPrefix = getCtrlIdPrefix();
    // CHECKSTYLE.ON: ConstantName

    private static long getCtrlIdPrefix() {
        try {
            final int javaId = 1;
            final int ipHash = xMsgUtil.localhost().hashCode() & Integer.MAX_VALUE;
            return javaId * 100000000 + (ipHash % 1000) * 100000;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String getCtrlId() {
        return Long.toString(ctrlIdPrefix + randomGenerator.nextInt(100000));
    }
}
