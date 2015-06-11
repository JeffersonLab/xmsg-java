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

package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
import java.net.SocketException;

/**
 * An example of a subscriber. It will receive any message of the given topic
 * published by existing or new publishers.
 * It also includes an inner class presenting the callback to be executed at
 * every arrival of the data.
 *
 * @author gurjyan
 */
public class Subscriber extends xMsg {
    private static xMsgConnection con;
    private MyCallBack callback;

    public Subscriber() throws xMsgException, SocketException {
        super("localhost");
        callback = new MyCallBack();
    }

    public static void main(String[] args) {
        try {
            final String myName = "test_subscriber";
            final String domain = "test_domain";
            final String subject = "test_subject";
            final String type = "test_type";
            final String description = "test_description";

            Subscriber subscriber = new Subscriber();

            // Create the connection to the local xMsg node
            con = subscriber.connect();

            // Create the topic
            xMsgTopic topic = xMsgTopic.build(domain, subject, type);

            // Register this subscriber
            subscriber.registerSubscriber(myName, topic, description);

            // Subscribe by passing a callback to the subscription
            subscriber.subscribe(con, topic.toString(), subscriber.callback);

            xMsgUtil.keepAlive();
        } catch (xMsgException | SocketException e) {
            e.printStackTrace();
        }
    }

    public void reply(xMsgMessage msg) {
        try {
            publish(con, msg);
        } catch (xMsgException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private class MyCallBack implements xMsgCallBack {
        long nr = 0;
        long t1;
        long t2;

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
            if (msg.getMetaData().getReplyTo().equals(xMsgConstants.UNDEFINED.getStringValue())) {
                xMsgData.Builder data = (xMsgData.Builder) msg.getData();
                if (nr == 0) {
                    t1 = System.currentTimeMillis();
                }
                nr = nr + 1;
                if (nr >= 10000) {
                    t2 = System.currentTimeMillis();
                    long dt = t2 - t1;
                    double pt = (double) dt / (double) nr;
                    long pr = (nr * 1000) / dt;
                    System.out.println("transfer time = " + pt + " ms");
                    System.out.println("transfer rate = " + pr + " Hz");
                    nr = 0;
                }
            } else {
                // sync request, create/update the xMsgMessage and send it to the sender
                reply(msg);
            }
            return msg;
        }
    }
}

