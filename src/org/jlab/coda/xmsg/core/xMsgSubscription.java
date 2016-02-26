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
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.io.IOException;

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

    // 0MQ sub socket
    private final Socket socket;

    // string of the topic
    private final String topic;

    // thread to wait for the published message and run the handle
    private final Thread thread;

    // the name for the subscription
    private final String name;

    // handle to stop the subscription
    private volatile boolean isRunning = false;


    /**
     * Creates a long-running subscription that process messages on the background.
     *
     * @see xMsg#subscribe
     */
    xMsgSubscription(String name, xMsgConnection connection, xMsgTopic topic) {
        this.name = name;
        this.socket = connection.getSubSock();
        if (this.socket == null) {
            throw new IllegalArgumentException("xMsg-Error: null subscription socket");
        }

        this.topic = topic.toString();
        subscribe(connection.getPubSock(), this.socket, this.topic);
        xMsgUtil.sleep(10);

        this.thread = xMsgUtil.newThread(name, new Handler());
    }


    /**
     * Used internally for a response subscription.
     *
     * @see xMsg#syncPublish
     */
    xMsgSubscription(xMsgConnection connection, xMsgTopic topic) {
        this.name = topic.toString();
        this.socket = connection.getSubSock();
        if (this.socket == null) {
            throw new IllegalArgumentException("xMsg-Error: null subscription socket");
        }

        this.topic = topic.toString();
        subscribe(connection.getPubSock(), this.socket, this.topic);
        xMsgUtil.sleep(10);

        this.thread = new Thread();
    }

    /**
     * Process a received message.
     *
     * @param msg {@link org.zeromq.ZMsg} object of the wire
     * @throws xMsgException
     * @throws TimeoutException
     * @throws IOException
     */
    abstract void handle(xMsgMessage msg) throws xMsgException, IOException;


    /**
     * Subscribes the connection and waits for a control message to confirm
     * the subscription.
     */
    private void subscribe(Socket pubSocket, Socket subSocket, String topic) {
        this.socket.subscribe(topic.toString().getBytes());

        ZMQ.Poller items = new ZMQ.Poller(1);
        items.register(subSocket, ZMQ.Poller.POLLIN);
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
        throw new RuntimeException("Could not subscribe to " + topic);
    }


    /**
     * Receives messages and runs user's callback.
     */
    private class Handler implements Runnable {

        @Override
        public void run() {
            ZMQ.Poller items = new ZMQ.Poller(1);
            items.register(socket, ZMQ.Poller.POLLIN);
            while (isRunning) {
                try {
                    items.poll(100);
                    if (items.pollin(0)) {
                        ZMsg msg = ZMsg.recvMsg(socket);
                        try {
                            if (msg.size() == 2) {
                                // ignore control message
                                // (which are composed of 2 frames)
                                continue;
                            }
                            handle(new xMsgMessage(msg));
                        } catch (xMsgException | IOException e) {
                            e.printStackTrace();
                        } finally {
                            msg.destroy();
                        }
                    }
                } catch (ZMQException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Starts the subscription thread.
     */
    void start() {
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
        socket.unsubscribe(topic.getBytes());
    }

    /**
     * Indicates if the subscription thread is running.
     */
    public boolean isAlive() {
        return isRunning;
    }

    /**
     * Returns the name of this subscription.
     */
    public String getName() {
        return name;
    }
}
