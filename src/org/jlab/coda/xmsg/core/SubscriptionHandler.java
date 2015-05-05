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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 *     xMsg subscription handler
 * </p>
 *
 * @author gurjyan
 * @version 2.x
 * @since 3/6/15
 */


public abstract class SubscriptionHandler implements Runnable {

    private boolean isRunning = true;
    private Socket con;
    private String topic;
    public SubscriptionHandler(xMsgConnection connection,
                               String topic){
        con = connection.getSubSock();
        this.topic = topic;
    }

    public abstract void handle() throws xMsgException, TimeoutException, IOException;

    @Override
    public void run() {
        while(isRunning){
            try {
                handle();
            } catch (xMsgException | TimeoutException | IOException e) {
                e.printStackTrace();
            }
        }
        con.unsubscribe(topic.getBytes(ZMQ.CHARSET));
    }

    public void unsubscribe(){
        isRunning = false;
    }

}
