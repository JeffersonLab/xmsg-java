package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

class DataSubscription {

    final xMsgConnection connection;
    final String topic;
    final Poller items;

    DataSubscription(xMsgConnection connection, xMsgTopic topic) throws xMsgException {
        this.connection = connection;
        this.topic = topic.toString();
        this.items = new Poller(1);

        start();
    }

    private void start() throws xMsgException {
        items.register(connection.getSubSock(), Poller.POLLIN);
        connection.subscribe(topic);
        if (!connection.checkSubscription(topic)) {
            throw new xMsgException("could not subscribe to " + topic);
        }
        xMsgUtil.sleep(10);
    }

    boolean hasMsg(int timeout) {
        int rc = items.poll(timeout);
        if (rc < 0) {
            throw new ZMQException(connection.getSubSock().base().errno());
        }
        return items.pollin(0);
    }

    ZMsg recvMsg() {
        return ZMsg.recvMsg(connection.getSubSock());
    }

    void stop() {
        connection.unsubscribe(topic);
    }
}
