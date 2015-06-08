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
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
import java.net.SocketException;

/**
 * An example of a publisher that publishes data for ever.
 * It does not matter who is subscribing to the messages.
 *
 * @author gurjyan
 * @version 2.x
 */
public class Publisher extends xMsg {

    /**
     * Constructor, requires the name of the front-end node.
     * The name is used to open a connection to the registrar service
     * running in the front-end.
     * <p>
     * In this case, localhost is expected to be the front-end.
     * Thus, an xMsg node must be running in the localhost.
     *
     * @throws xMsgException
     */
    public Publisher() throws xMsgException, SocketException {
        super("localhost");
    }

    public static void main(String[] args) {
        try {
            if (args.length != 1) {
                System.err.println("Usage: Publisher <data_size_kb>");
                System.exit(1);
            }

            int dataSize = Integer.parseInt(args[0]);

            final String name = "test_publisher";
            final String domain = "test_domain";
            final String subject = "test_subject";
            final String type = "test_type";
            final String description = "test_description";

            Publisher publisher = new Publisher();

            // Create a connection to the local xMsg node
            xMsgConnection con = publisher.connect();

            // Register this publisher
            publisher.registerPublisher(name, domain, subject, type, description);

            // Create the message to be published
            String topic = xMsgUtil.buildTopic(domain, subject, type);
            xMsgMessage msg = new xMsgMessage(topic);

            // Fill data with a byte array the required size
            System.out.println("Byte array size = " + dataSize);
            byte[] b = new byte[dataSize];
            msg.setData(b);

            // Publish data for ever...
            while (true) {
                publisher.publish(con, msg);
            }

        } catch (xMsgException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException e) {
            System.err.println("Parameter must be an integer (the data size in KB)!!");
            System.exit(1);
        }
    }
}
