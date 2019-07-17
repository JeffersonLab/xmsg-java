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
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgConnectionSetup;
import org.jlab.coda.xmsg.sys.pubsub.xMsgPoller;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A handler for a running subscription of specific topic(s).
 * A subscription uses a {@link xMsgConnection connection} to receive
 * {@link xMsgMessage messages} of the interested {@link xMsgTopic topic},
 * and calls a user action on every message.
 * <p>
 * When the subscription is created, the connection will be subscribed to
 * the topic, and a background thread will be started polling the connection for
 * received messages. For every message, the user-provide callback will be
 * executed.
 * <p>
 * When the subscription is destroyed, the background thread will be stopped
 * and the connection will be unsubscribed from the topic.
 * <p>
 * Creation and destruction of subscriptions are controlled by the xMsg actor.
 *
 * @version 2.x
 */
public abstract class xMsgSubscription {

    private final String name;
    private final xMsgProxyDriver connection;
    private final List<String> topics;

    private final Thread thread;
    private volatile boolean isRunning = false;

    /**
     * Creates a long-running subscription that process messages on the background.
     *
     * @see xMsg#subscribe
     */
    xMsgSubscription(String name, xMsgProxyDriver connection, Set<xMsgTopic> topics) {
        this.name = name;
        this.connection = connection;
        this.topics = topics.stream().map(xMsgTopic::toString).collect(Collectors.toList());
        this.thread = xMsgUtil.newThread(name, new Handler());
    }


    /**
     * Process a received message.
     *
     * @param msg the received message
     * @throws xMsgException if there was an error handling the message
     */
    abstract void handle(xMsgMessage msg) throws xMsgException;


    /**
     * Receives messages and runs user's callback.
     */
    private class Handler implements Runnable {

        @Override
        public void run() {
            try (xMsgPoller poller = new xMsgPoller(connection)) {
                waitMessages(poller);
            }
        }

        private void waitMessages(xMsgPoller poller) {
            while (isRunning) {
                try {
                    if (poller.poll(100)) {
                        ZMsg msg = connection.recv();
                        if (msg == null) {
                            break; // interrupted
                        }
                        try {
                            if (msg.size() == 2) {
                                // ignore control message
                                // (which are composed of 2 frames)
                                continue;
                            }
                            handle(new xMsgMessage(msg));
                        } catch (xMsgException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                        break;
                    }
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Starts the subscription thread.
     *
     * @throws xMsgException if subscription could not be started
     */
    void start(xMsgConnectionSetup setup) throws xMsgException {
        setup.preSubscription(connection.getSocket());
        topics.forEach(connection::subscribe);
        if (setup.checkSubscription()
                && !connection.checkSubscription(topics.get(0), setup.subscriptionTimeout())) {
            topics.forEach(connection::unsubscribe);
            throw new xMsgException(subscriptionError());
        }
        setup.postSubscription();
        isRunning = true;
        thread.start();
    }

    /**
     * Stops the background subscription thread and unsubscribes the socket.
     */
    void stop() {
        try {
            isRunning = false;
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            topics.forEach(connection::unsubscribe);
            connection.close();
        }
    }

    private String subscriptionError() {
        int size = topics.size();
        StringBuilder sb = new StringBuilder();
        sb.append("could not subscribe with ").append(connection.getAddress());
        sb.append(" [");
        sb.append(topics.get(0));
        if (size > 1) {
            sb.append(" and ").append(size).append(" others");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Gets the address of the proxy used by the subscription.
     *
     * @return the address of the proxy
     */
    public xMsgProxyAddress getAddress() {
        return connection.getAddress();
    }

    /**
     * Gets the set of subscribed topics of interest.
     *
     * @return the subscribed topics
     */
    public Set<xMsgTopic> getTopics() {
        return topics.stream().map(xMsgTopic::wrap).collect(Collectors.toSet());
    }

    /**
     * Indicates if the subscription thread is running.
     *
     * @return true if the subscription is running, false otherwise
     */
    public boolean isAlive() {
        return thread.isAlive();
    }

    String getName() {
        return name;
    }
}
