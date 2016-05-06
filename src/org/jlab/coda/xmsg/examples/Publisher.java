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
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegInfo;

/**
 * An example of a publisher that publishes data for ever.
 * It does not matter who is subscribing to the messages.
 * This publisher uses the  proxy, running
 * on a localhost, with the default poxy port to publish data.
 *
 * Published data is a int[] with a specified size.
 *
 * @author gurjyan
 * @version 2.x
 */
public class Publisher extends xMsg {

    public xMsgConnection con;
    public xMsgTopic topic;

    /**
     * Calls the parent constructor, connects to the
     * local proxy and creates a topic of conversation,
     * as well as registers with the local registrar.
     */
    public Publisher() throws xMsgException {
        super("test_publisher");

        // connect to default proxy (local host, default proxy port)
        con = getConnection();

        // build the publishing topic (hard codded)
        final String domain = "test_domain";
        final String subject = "test_subject";
        final String type = "test_type";
        final String description = "test_description";
        topic = xMsgTopic.build(domain, subject, type);

        // Register this publisher
        register(xMsgRegInfo.publisher(topic, description));

    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: Publisher <data_size in bytes>");
            System.exit(1);
        }

        // create a publisher object
        try (Publisher publisher = new Publisher()) {
            // get the data size to be sent periodically
            int dataSize = Integer.parseInt(args[0]);
            System.out.println("Byte array size = " + dataSize);

            // create a byte array the required size and set it in a new message
            byte[] b = new byte[dataSize];

            // note that this constructor will create a metadata automatically
            // based on the type of passed object. In this case since data
            // is of the int[] object, the metadata will be created with
            // data type = xMsgConstants.ARRAY_SFIXED32
            xMsgMessage msg = new xMsgMessage(publisher.topic, "data/binary", b);

            // Async publish data for ever
            while (true) {
                publisher.publish(publisher.con, msg);
            }
        } catch (xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException e) {
            System.err.println("Parameter must be an integer (the data size in bytes)!!");
            System.exit(1);
        }
    }
}
