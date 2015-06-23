/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
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
 * xMsg subscription handler.
 *
 * @author gurjyan
 * @version 2.x
 * @since 3/6/15
 */
public abstract class xMsgSubscription {

    private final Socket socket;
    private final String topic;
    private final Thread thread;

    private volatile boolean isRunning = true;


    xMsgSubscription(String name, xMsgConnection connection, xMsgTopic topic) {
        this.socket = connection.getSubSock();
        if (this.socket == null) {
            throw new IllegalArgumentException("Error: null subscription socket");
        }
        this.socket.subscribe(topic.toString().getBytes(ZMQ.CHARSET));
        xMsgUtil.sleep(100);

        this.topic = topic.toString();
        this.thread = xMsgUtil.newThread(name, new Handler());
    }


    abstract void handle(ZMsg msg) throws xMsgException, TimeoutException, IOException;


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


    void start() {
        thread.start();
    }


    void stop() {
        isRunning = false;
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        socket.unsubscribe(topic.getBytes(ZMQ.CHARSET));
    }


    public boolean isAlive() {
        return isRunning;
    }
}
