package org.jlab.coda.xmsg.net;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Random;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

public class xMsgConnectionFactory {

    private ZContext context;

    public xMsgConnectionFactory(ZContext context) {
        this.context = context;

        // fix default linger
        this.context.setLinger(-1);
    }

    public xMsgConnection createProxyConnection(xMsgProxyAddress address,
                                                xMsgConnectionSetup setup) {
        Socket pubSock = context.createSocket(ZMQ.PUB);
        Socket subSock = context.createSocket(ZMQ.SUB);
        Socket ctrlSock = context.createSocket(ZMQ.DEALER);

        setup.preConnection(pubSock);
        setup.preConnection(subSock);

        int pubPort = address.port();
        int subPort = pubPort + 1;
        int ctrlPort = subPort + 1;

        String identity = getCtrlId();
        ctrlSock.setIdentity(identity.getBytes());

        pubSock.connect("tcp://" + address.host() + ":" + pubPort);
        subSock.connect("tcp://" + address.host() + ":" + subPort);
        ctrlSock.connect("tcp://" + address.host() + ":" + ctrlPort);

        if (!checkConnection(pubSock, ctrlSock, identity)) {
            context.destroySocket(pubSock);
            context.destroySocket(subSock);
            context.destroySocket(ctrlSock);
            throw new RuntimeException("Could not connect to " + address.toString());
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

    public xMsgRegDriver createRegistrarConnection(xMsgRegAddress address) {
        Socket socket = context.createSocket(ZMQ.REQ);
        socket.setHWM(0);
        socket.connect("tcp://" + address.host() + ":" + address.port());
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


    private boolean checkConnection(Socket pubSocket, Socket ctrlSocket, String identity) {
        ZMQ.Poller items = new ZMQ.Poller(1);
        items.register(ctrlSocket, ZMQ.Poller.POLLIN);
        int retry = 0;
        while (retry <= 10) {
            retry++;
            ZMsg ctrlMsg = new ZMsg();
            try {
                ctrlMsg.add(xMsgConstants.CTRL_TOPIC);
                ctrlMsg.add(xMsgConstants.CTRL_CONNECT);
                ctrlMsg.add(identity);
                ctrlMsg.send(pubSocket);

                items.poll(10);
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
            return (ipHash % 1000) * 1000000 + javaId * 100000;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String getCtrlId() {
        return Long.toString(ctrlIdPrefix + randomGenerator.nextInt(100000));
    }
}
