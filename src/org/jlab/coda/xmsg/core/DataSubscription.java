package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgPoller;
import org.zeromq.ZMsg;

class DataSubscription {

    final xMsgConnection connection;
    final xMsgPoller poller;
    final String topic;

    DataSubscription(xMsgConnection connection, xMsgTopic topic) throws xMsgException {
        this.connection = connection;
        this.poller = new xMsgPoller(connection);
        this.topic = topic.toString();

        start();
    }

    private void start() throws xMsgException {
        connection.subscribe(topic);
        if (!connection.checkSubscription(topic)) {
            throw new xMsgException("could not subscribe to " + topic);
        }
        xMsgUtil.sleep(10);
    }

    boolean hasMsg(int timeout) {
        return poller.poll(timeout);
    }

    ZMsg recvMsg() {
        return connection.recv();
    }

    void stop() {
        connection.unsubscribe(topic);
    }
}
