/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * xMsg subscription handler. This gets the sub socket from the xMsgConnection
 * input parameter, calls the subscribe method of the socket, passing the topic
 * of the subscription. Then it starts a separate thread to start polling the
 * socket for the publishing results. As soon as the results arrive from a
 * publisher the abstract method: handle of this method will be called.
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
    private String name;

    // handle to stop the subscription
    private volatile boolean isRunning = true;


    /**
     * xMsg subscription constructor.
     *
     * @param name       the name of the subscription
     * @param connection the {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     * @param topic      {@link org.jlab.coda.xmsg.core.xMsgTopic} object
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
     * An abstract method to be implemented by the user's subscription callback.
     *
     * @param msg {@link org.zeromq.ZMsg} object of the wire
     * @throws xMsgException
     * @throws TimeoutException
     * @throws IOException
     */
    abstract void handle(ZMsg msg) throws xMsgException, TimeoutException, IOException;


    /**
     * Starts the subscription thread.
     */
    void start() {
        thread.start();
    }

    /**
     * Stops the running subscription thread. This also
     * calls unsubscribe method of the 0MQ socket:
     * {@link org.zeromq.ZMQ.Socket#unsubscribe(byte[])}
     */
    void stop() {
        isRunning = false;
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        socket.unsubscribe(topic.getBytes());
    }

    /**
     * Returns this subscription name.
     *
     * @return the name of the subscription
     */
    public String getName() {
        return name;
    }

    /**
     * Indicates if the subscription thread is running.
     * @return true/false
     */
    public boolean isAlive() {
        return isRunning;
    }

    private void subscribe(Socket pubSocket, Socket subSocket, String topic) {
        this.socket.subscribe(topic.toString().getBytes());

        ZMQ.Poller items = new ZMQ.Poller(1);
        items.register(subSocket, ZMQ.Poller.POLLIN);
        int retry = 0;
        while (retry <= 10) {
            retry++;
            ZMsg ctrlMsg = new ZMsg();
            try {
                ctrlMsg.add(xMsgConstants.CTRL_TOPIC);
                ctrlMsg.add(xMsgConstants.CTRL_SUBSCRIBE);
                ctrlMsg.add(topic);
                ctrlMsg.send(pubSocket);

                items.poll(10);
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
     * Private runnable that runs user's subscription callback.
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
                            handle(msg);
                        } catch (xMsgException | TimeoutException | IOException e) {
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
}
