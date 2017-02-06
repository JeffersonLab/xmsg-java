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

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgRegInfo;

/**
 * An example of a publisher that publishes data for ever.
 * It does not matter who is subscribing to the messages.
 * This publisher uses the default proxy running on localhost.
 *
 * Published data is a byte array with a specified size.
 *
 * @version 2.x
 */
public class Publisher extends xMsg {

    Publisher() {
        super("test_publisher");
    }

    public static void main(String[] args) {

        if (args.length != 1) {
            System.err.println("Usage: publisher <data_size_bytes>");
            System.exit(1);
        }

        // create a publisher object
        try (Publisher publisher = new Publisher()) {

            // build the publishing topic (hard-coded)
            String domain = "test_domain";
            String subject = "test_subject";
            String type = "test_type";
            String description = "test_description";
            xMsgTopic topic = xMsgTopic.build(domain, subject, type);

            // register this publisher
            publisher.register(xMsgRegInfo.publisher(topic, description));

            // get the data size to be sent periodically
            int dataSize = Integer.parseInt(args[0]);
            System.out.printf("Byte array size = %d%n", dataSize);

            // create a byte array the required size and set it in a new message
            byte[] b = new byte[dataSize];

            // create the data message to the specified topic
            xMsgMessage msg = new xMsgMessage(topic, "data/binary", b);

            System.out.printf("Publishing to = %s%n", topic);

            // connect to the local proxy
            try (xMsgConnection con = publisher.getConnection()) {
                // publish data for ever
                while (true) {
                    publisher.publish(con, msg);
                }
            }
        } catch (NumberFormatException e) {
            System.err.println("Parameter must be an integer (the data size in bytes)!!");
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
