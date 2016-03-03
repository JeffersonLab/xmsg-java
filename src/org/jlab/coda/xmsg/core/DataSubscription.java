package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

class DataSubscription {

    final Socket pubSocket;
    final Socket subSocket;
    final String topic;
    final Poller items;

    DataSubscription(xMsgConnection connection, xMsgTopic topic) throws xMsgException {
        this.pubSocket = connection.getPubSock();
        this.subSocket = connection.getSubSock();
        this.topic = topic.toString();
        this.items = new Poller(1);

        start();
        xMsgUtil.sleep(10);
    }

    private void start() throws xMsgException {
        this.subSocket.subscribe(topic.getBytes());
        this.items.register(subSocket, Poller.POLLIN);

        int retry = 0;
        while (retry < 10) {
            retry++;
            ZMsg ctrlMsg = new ZMsg();
            try {
                ctrlMsg.add(xMsgConstants.CTRL_TOPIC);
                ctrlMsg.add(xMsgConstants.CTRL_SUBSCRIBE);
                ctrlMsg.add(topic);
                ctrlMsg.send(pubSocket);

                items.poll(100);
                if (items.pollin(0)) {
                    ZMsg replyMsg = ZMsg.recvMsg(subSocket);
                    try {
                        // TODO: check the message
                        return;
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
        throw new xMsgException("Could not subscribe to " + topic);
    }

    boolean hasMsg(int timeout) {
        items.poll(timeout);
        return items.pollin(0);
    }

    ZMsg recvMsg() {
        return ZMsg.recvMsg(subSocket);
    }

    void stop() {
        subSocket.unsubscribe(topic.getBytes());
    }
}
