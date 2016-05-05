package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.jlab.coda.xmsg.net.xMsgListener;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgProxyDriver;
import org.zeromq.ZMsg;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

class ResponseListener extends xMsgListener {

    private final xMsgConnectionFactory factory;
    private final String topic;

    private final ConcurrentMap<String, xMsgMessage> responses;

    ResponseListener(String id, xMsgConnectionFactory factory) {
        super("poll-" + id);
        this.factory = factory;
        this.topic = xMsgTopic.build("ret", id).toString();
        this.responses = new ConcurrentHashMap<>();
    }

    public void register(xMsgProxyAddress address) throws xMsgException {
        if (items.get(address) == null) {
            xMsgConnectionSetup setup = new xMsgConnectionSetup() { };
            xMsgProxyDriver connection = factory.createProxyConnection(address, setup);
            connection.subscribe(topic);
            if (!connection.checkSubscription(topic.toString())) {
                connection.close();
                throw new xMsgException("could not subscribe to " + topic);
            }
            xMsgProxyDriver value = items.putIfAbsent(address, connection);
            if (value != null) {
                connection.unsubscribe(topic);
                connection.close();
            }
        }
    }

    public xMsgMessage waitMessage(String topic, int timeout) throws TimeoutException {
        int t = 0;
        while (t < timeout) {
            xMsgMessage repMsg = responses.remove(topic);
            if (repMsg != null) {
                return repMsg;
            }
            xMsgUtil.sleep(1);
            t += 1;
        }
        throw new TimeoutException("Error: no response for time_out = " + t);
    }

    @Override
    public void handle(ZMsg rawMsg) throws xMsgException {
        xMsgMessage msg = new xMsgMessage(rawMsg);
        responses.put(msg.getTopic().toString(), msg);
    }
}
