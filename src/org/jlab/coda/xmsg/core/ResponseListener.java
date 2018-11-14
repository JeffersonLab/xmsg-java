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

package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgConnectionSetup;
import org.jlab.coda.xmsg.sys.pubsub.xMsgListener;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.zeromq.ZMsg;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

class ResponseListener extends xMsgListener {

    private final xMsgConnectionFactory factory;
    private final String topic;

    private final ConcurrentMap<String, xMsgMessage> responses;

    ResponseListener(String id, xMsgConnectionFactory factory) {
        super("poll-" + id, factory.getContext());
        this.factory = factory;
        this.topic = xMsgTopic.build("ret", id).toString();
        this.responses = new ConcurrentHashMap<>();
    }

    public void register(xMsgProxyAddress address) throws xMsgException {
        if (items.get(address) == null) {
            xMsgConnectionSetup setup = xMsgConnectionSetup.newBuilder().build();
            xMsgProxyDriver connection = factory.createSubscriberConnection(address, setup);
            connection.subscribe(topic);
            if (!connection.checkSubscription(topic, setup.subscriptionTimeout())) {
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

    public xMsgMessage waitMessage(String topic, long timeout) throws TimeoutException {
        int t = 0;
        while (t < timeout) {
            xMsgMessage repMsg = responses.remove(topic);
            if (repMsg != null) {
                return repMsg;
            }
            xMsgUtil.sleep(1);
            t += 1;
        }
        throw new TimeoutException("no response for timeout = " + t);
    }

    @Override
    public void handle(ZMsg rawMsg) throws xMsgException {
        xMsgMessage msg = new xMsgMessage(rawMsg);
        responses.put(msg.getTopic().toString(), msg);
    }
}
