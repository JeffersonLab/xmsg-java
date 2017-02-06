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
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM;
import org.jlab.coda.xmsg.data.xMsgMimeType;
import org.jlab.coda.xmsg.data.xMsgRegInfo;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.util.List;

/**
 * An example of a subscriber. It will receive any message of the given topic
 * published by existing or new publishers.
 * It also includes an inner class presenting the callback to be executed at
 * every arrival of the data.
 *
 * @version 2.x
 */
public class Subscriber extends xMsg {

    Subscriber() {
        super("test_subscriber", 1);
    }

    /**
     * Subscribes to a hard-coded topic on the local proxy,
     * and registers with the local registrar.
     */
    void start() throws xMsgException  {
        // build the subscribing topic (hard-coded)
        String domain = "test_domain";
        String subject = "test_subject";
        String type = "test_type";
        String description = "test_description";
        xMsgTopic topic = xMsgTopic.build(domain, subject, type);

        // subscribe to default local proxy
        subscribe(topic, new MyCallBack());

        // register with the local registrar
        register(xMsgRegInfo.subscriber(topic, description));

        System.out.printf("Subscribed to = %s%n", topic);
    }

    public static void main(String[] args) {
        try (Subscriber sub = new Subscriber()) {
            sub.start();
            xMsgUtil.keepAlive();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Publishes a message using the same proxy used to receive the message.
     * This method is used in case the request (the publisher)
     * publishes data in sync ( required a response back).
     *
     * @param msg a received message
     */
    private void respondBack(xMsgMessage msg, Object data) {
        try {
            publish(xMsgMessage.createResponse(msg, data));
        } catch (xMsgException e) {
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
         * @param msg a received message
         * @return data of the message, otherwise null
         */
        private List<Integer> parseData(xMsgMessage msg) {
            try {
                xMsgM.xMsgMeta.Builder metadata = msg.getMetaData();
                if (metadata.getDataType().equals(xMsgMimeType.ARRAY_SFIXED32)) {
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
