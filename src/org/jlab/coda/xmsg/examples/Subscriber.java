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

import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
import java.net.SocketException;

/**
 * xMsg subscriber that checks the local registration to find
 * out if there is a publisher publishing to a specified
 * domain, subject and type. In case publisher is found subscribes
 * the published data. It also includes the inner class
 * presenting the callback to be executed at every arrival od the
 * data. Callback does simple print of the received message.
 *
 * @author gurjyan
 * @version 4.x
 * @since 11/4/14
 */
public class Subscriber extends xMsg {
    private static final String myName = "test_subscriber";
    private static final String domain = "test_domain";
    private static final String subject = "test_subject";
    private static final String type = "test_type";
    private static final String description = "test_description";
    private static xMsgConnection con;
    private MyCallBack callback;

    public Subscriber() throws xMsgException, SocketException {
        super("localhost");
        callback = new MyCallBack();
    }

    public static void main(String[] args) {
        try {
            Subscriber subscriber = new Subscriber();

            // Create a socket connections to the xMsg node
            con = subscriber.connect();

            // Register this subscriber
            subscriber.registerSubscriber(myName, domain, subject, type, description);

            // Find a publisher that publishes to requested topic
            // defined as a static variables above
//            if (subscriber.isThereLocalPublisher(myName, domain, subject, type)){

                // Subscribe by passing a callback to the subscription
            String topic = xMsgUtil.buildTopic(domain, subject, type);
            subscriber.subscribe(con, topic, subscriber.callback);

                xMsgUtil.keepAlive();

//            }
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

    private class MyCallBack implements xMsgCallBack{
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
                // sync request, create/update the xMsgMessage and sen it to the sender
                reply(msg);

            }
//            System.out.println(data.getFLSINT32());
            return msg;
        }
    }
}

