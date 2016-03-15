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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

public class xMsgConnectionFactory {

    private final ZContext context;
    private final xMsgSocketFactory factory;

    public xMsgConnectionFactory(ZContext context) {
        this.context = context;
        this.factory = new xMsgSocketFactory(context);

        // fix default linger
        this.context.setLinger(-1);
    }

    public xMsgConnection createProxyConnection(xMsgProxyAddress address,
                                                xMsgConnectionSetup setup) throws xMsgException {

        Socket pubSock = factory.createSocket(ZMQ.PUB);
        Socket subSock = factory.createSocket(ZMQ.SUB);
        Socket ctrlSock = factory.createSocket(ZMQ.DEALER);

        String identity = IdentityGenerator.getCtrlId();
        ctrlSock.setIdentity(identity.getBytes());

        try {
            setup.preConnection(pubSock);
            setup.preConnection(subSock);

            int pubPort = address.port();
            int subPort = pubPort + 1;
            int ctrlPort = subPort + 1;

            factory.connectSocket(pubSock, address.host(), pubPort);
            factory.connectSocket(subSock, address.host(), subPort);
            factory.connectSocket(ctrlSock, address.host(), ctrlPort);

            if (!checkConnection(pubSock, ctrlSock, identity)) {
                throw new xMsgException("Could not connect to " + address);
            }
            setup.postConnection();

            return new xMsgConnection(address, identity, pubSock, subSock, ctrlSock);

        } catch (ZMQException | xMsgException e) {
            factory.destroySocket(pubSock);
            factory.destroySocket(subSock);
            factory.destroySocket(ctrlSock);
            throw e;
        }
    }

    public xMsgRegDriver createRegistrarConnection(xMsgRegAddress address) throws xMsgException {
        Socket socket = factory.createSocket(ZMQ.REQ);
        try {
            socket.setHWM(0);
            factory.connectSocket(socket, address.host(), address.port());
            return new xMsgRegDriver(address, socket);
        } catch (ZMQException | xMsgException e) {
            factory.destroySocket(socket);
            throw e;
        }
    }

    public void destroyProxyConnection(xMsgConnection connection) {
        factory.destroySocket(connection.getPubSock());
        factory.destroySocket(connection.getSubSock());
    }

    public void destroyRegistrarConnection(xMsgRegDriver connection) {
        factory.destroySocket(connection.getSocket());
    }

    public void setLinger(int linger) {
        context.setLinger(linger);
    }

    public void destroy() {
        if (!context.isMain()) {
            context.destroy();
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
                        if (replyMsg.size() == 1) {
                            ZFrame typeFrame = replyMsg.pop();
                            try {
                                String type = new String(typeFrame.getData());
                                if (type.equals(xMsgConstants.CTRL_CONNECT)) {
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
            } catch (ZMQException e) {
                e.printStackTrace();
            } finally {
                ctrlMsg.destroy();
            }
        }
        return false;
    }
}
