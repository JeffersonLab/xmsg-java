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

package org.jlab.coda.xmsg.examples;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
import java.util.List;

/**
 * An example of a subscriber. It will receive any message of the given topic
 * published by existing or new publishers.
 * It also includes an inner class presenting the callback to be executed at
 * every arrival of the data.
 *
 * @author gurjyan
 * @version 2.x
 */
public class Subscriber extends xMsg {

    xMsgConnection con;
    xMsgTopic topic;

    /**
     * Calls the parent constructor.
     * Registers with a local registrar.
     * subscribes to a hardcoded topic.
     *
     * @throws IOException
     */
    public Subscriber() throws IOException, xMsgException {
        super("test_subscriber", 1);

        // connect to default proxy (local host, default proxy port)
        con = createConnection();

        // build the subscribing topic (hard codded)
        final String domain = "test_domain";
        final String subject = "test_subject";
        final String type = "test_type";
        final String description = "test_description";
        topic = xMsgTopic.build(domain, subject, type);

        // Register this subscriber
        registerAsSubscriber(topic, description);

        // Subscribe by passing a callback to the subscription
        subscribe(con, topic, new MyCallBack());
        System.out.printf("Subscribed to = %s%n", topic);

    }

    public static void main(String[] args) {
        try (Subscriber subscriber = new Subscriber()) {
            xMsgUtil.keepAlive();
        } catch (xMsgException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Publishes a message using the same proxy used to receive the message.
     * This method is used in case the request (the publisher)
     * publishes data in sync ( required a response back).
     *
     * @param msg {@link org.jlab.coda.xmsg.core.xMsgMessage} object
     */
    public void respondBack(xMsgMessage msg, Object data) {
        try {
            publish(con, xMsgMessage.createResponse(msg, data));
        } catch (xMsgException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Private callback class.
     */
    private class MyCallBack implements xMsgCallBack {

        // variables for naive benchmarking
        long nr = 0;
        long t1;
        long t2;

        @Override
        public void callback(xMsgMessage msg) {
            if (!msg.getMetaData().hasReplyTo()) {
                // we get the data, but will not do anything with it for
                // communication benchmarking purposes.
                /* List<Integer> data = */ parseData(msg);

                if (nr == 0) {
                    t1 = System.currentTimeMillis();
                }
                nr = nr + 1;
                if (nr >= 10000) {
                    t2 = System.currentTimeMillis();
                    long dt = t2 - t1;
                    double pt = (double) dt / (double) nr;
                    long pr = (nr * 1000) / dt;
                    System.out.println();
                    System.out.printf("transfer time = %.3f [ms]%n", pt);
                    System.out.printf("transfer rate = %d [Hz]%n", pr);
                    nr = 0;
                }
            } else {
                // sends back "Done" string
                respondBack(msg, "Done");
            }
        }

        /**
         * De-serializes received message and retrieves List of integers
         * Note this method is not checking the metadata for the mimeType.
         *
         * @param msg {@link org.jlab.coda.xmsg.core.xMsgMessage} object
         * @return data of the message, otherwise null
         */
        private List<Integer> parseData(xMsgMessage msg) {
            try {
                xMsgM.xMsgMeta.Builder metadata = msg.getMetaData();
                if (metadata.getDataType().equals(xMsgConstants.MimeType.ARRAY_SFIXED32)) {
                    xMsgData data = xMsgData.parseFrom(msg.getData());
                    return data.getFLSINT32AList();
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}

