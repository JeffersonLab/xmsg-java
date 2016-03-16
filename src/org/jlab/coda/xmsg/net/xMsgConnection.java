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
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

/**
 * The standard connection to xMsg nodes.
 * Contains xMsgAddress object and two 0MQ sockets for publishing and
 * subscribing xMsg messages respectfully.
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgConnection {

    private final xMsgProxyAddress address;
    private final String identity;
    private final Socket pubSocket;
    private final Socket subSocket;
    private final Socket ctrlSocket;

    private final xMsgSocketFactory factory;

    xMsgConnection(xMsgProxyAddress address,
                   xMsgSocketFactory factory) throws xMsgException {
        this.address = address;
        this.identity = IdentityGenerator.getCtrlId();
        this.pubSocket = factory.createSocket(ZMQ.PUB);
        this.subSocket = factory.createSocket(ZMQ.SUB);
        this.ctrlSocket = factory.createSocket(ZMQ.DEALER);
        this.factory = factory;

        this.ctrlSocket.setIdentity(this.identity.getBytes());
    }

    public void connect() throws xMsgException {
        int pubPort = address.port();
        int subPort = pubPort + 1;
        int ctrlPort = subPort + 1;

        factory.connectSocket(pubSocket, address.host(), pubPort);
        factory.connectSocket(subSocket, address.host(), subPort);
        factory.connectSocket(ctrlSocket, address.host(), ctrlPort);
    }

    public boolean checkConnection() {
        Poller items = new Poller(1);
        items.register(ctrlSocket, Poller.POLLIN);
        int retry = 0;
        while (retry < 10) {
            try {
                retry++;
                ZMsg ctrlMsg = new ZMsg();
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
            }
        }
        return false;
    }

    public void close() {
        factory.destroySocket(pubSocket);
        factory.destroySocket(subSocket);
        factory.destroySocket(ctrlSocket);
    }

    public xMsgProxyAddress getAddress() {
        return address;
    }

    public String getIdentity() {
        return identity;
    }

    public Socket getPubSock() {
        return pubSocket;
    }

    public Socket getSubSock() {
        return subSocket;
    }

    public Socket getControlSock() {
        return ctrlSocket;
    }
}
