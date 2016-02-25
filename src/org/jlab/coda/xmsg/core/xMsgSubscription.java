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
import org.zeromq.ZMQException;

import java.io.IOException;

/**
 * xMsg subscription handler.
 *
 * @author gurjyan
 * @version 2.x
 * @since 3/6/15
 */
public abstract class xMsgSubscription {

    private final Thread thread;
    private final DataSubscription sub;

    private volatile boolean isRunning = false;

    xMsgSubscription(String name, xMsgConnection connection, xMsgTopic topic) throws xMsgException {
        this.sub = new DataSubscription(connection, topic);
        this.thread = xMsgUtil.newThread(name, new Handler());
    }


    abstract void handle(xMsgMessage msg) throws xMsgException, IOException;


    private class Handler implements Runnable {

        @Override
        public void run() {
            while (isRunning) {
                try {
                    if (sub.hasMsg(100)) {
                        try {
                            handle(sub.recvMsg());
                        } catch (xMsgException | IOException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (ZMQException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    void start() {
        isRunning = true;
        thread.start();
    }


    void stop() {
        try {
            isRunning = false;
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sub.stop();
    }


    public boolean isAlive() {
        return isRunning;
    }
}
