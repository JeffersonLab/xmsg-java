package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

class DataSubscription {

    final Socket socket;
    final String topic;
    final Poller items;

    DataSubscription(xMsgConnection connection, xMsgTopic topic) throws xMsgException {
        this(connection, topic.toString());
    }

    DataSubscription(xMsgConnection connection, String topic) throws xMsgException {
        this.socket = connection.getSubSock();
        this.topic = topic.toString();
        this.items = new Poller(1);

        start();
    }

    private void start() {
        this.socket.subscribe(topic.getBytes());
        xMsgUtil.sleep(100);
        this.items.register(socket, Poller.POLLIN);
    }

    boolean hasMsg(int timeout) {
        items.poll(timeout);
        return items.pollin(0);
    }

    xMsgMessage recvMsg() throws xMsgException {
        ZMsg rawMsg = ZMsg.recvMsg(socket);
        try {
            return new xMsgMessage(rawMsg);
        } finally {
            rawMsg.destroy();
        }
    }

    void stop() {
        socket.unsubscribe(topic.getBytes());
    }
}
