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
import org.jlab.coda.xmsg.sys.pubsub.xMsgPoller;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

/**
 * A subscription object uses a {@link xMsgConnection connection} to receive
 * {@link xMsgMessage messages} of the interested {@link xMsgTopic topic},
 * and calls a user action on every message.
 * <p>
 * When the subscription is constructed, the connection will be subscribed to
 * the topic, and a background thread will be started polling the connection for
 * received messages. For every message, the user-provide callback will be
 * executed.
 * <p>
 * When the subscription is destroyed, the background thread will be stopped
 * and the connection will be unsubscribed from the topic.
 * <p>
 * Creation and destruction of subscriptions are controlled by the xMsg actor.
 *
 * @author gurjyan
 * @version 2.x
 */
public abstract class xMsgSubscription {

    private final String name;
    private final xMsgProxyDriver connection;
    private final String topic;

    private final Thread thread;
    private volatile boolean isRunning = false;

    /**
     * Creates a long-running subscription that process messages on the background.
     *
     * @see xMsg#subscribe
     */
    xMsgSubscription(String name, xMsgProxyDriver connection, xMsgTopic topic) {
        this.name = name;
        this.connection = connection;
        this.topic = topic.toString();
        this.thread = xMsgUtil.newThread(name, new Handler());
    }


    /**
     * Process a received message.
     *
     * @param msg {@link org.zeromq.ZMsg} object of the wire
     * @throws xMsgException
     * @throws TimeoutException
     */
    abstract void handle(xMsgMessage msg) throws xMsgException;


    /**
     * Receives messages and runs user's callback.
     */
    private class Handler implements Runnable {

        @Override
        public void run() {
            xMsgPoller poller = new xMsgPoller(connection);
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
                        } finally {
                            msg.destroy();
                        }
                    }
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                        break;
                    }
                    e.printStackTrace();
                }
            }
            connection.unsubscribe(topic);
            connection.close();
        }
    }

    /**
     * Starts the subscription thread.
     *
     * @throws xMsgException if subscription could not be started
     */
    void start() throws xMsgException {
        connection.subscribe(topic);
        if (!connection.checkSubscription(topic)) {
            connection.unsubscribe(topic);
            throw new xMsgException("could not subscribe to " + topic);
        }
        xMsgUtil.sleep(10);
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
        }
    }

    /**
     * Indicates if the subscription thread is running.
     */
    public boolean isAlive() {
        return thread.isAlive();
    }

    /**
     * Returns the name of this subscription.
     */
    public String getName() {
        return name;
    }
}
