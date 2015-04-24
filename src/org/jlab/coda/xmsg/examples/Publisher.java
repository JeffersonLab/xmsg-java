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
import java.util.Random;

/**
 * An example of a publisher that publishes
 * data required size in kBytes for ever.
 * He does not care who is subscribing to his
 * data.
 *
 * @author gurjyan
 * @version 1.x
 * @since 11/4/14
 */
public class Publisher extends xMsg {
    private static final String myName = "test_publisher";
    private static final String domain = "test_domain";
    private static final String subject = "test_subject";
    private static final String type = "test_type";

    /**
     * <p>
     * Constructor, requires the name of the FrontEnd host that is used
     * to create a connection to the registrar service running within the
     * xMsgFE. Creates the zmq context object and thread pool for servicing
     * received messages in a separate threads.
     * Localhost is considered to be the host of the FE.
     * Thread pool is relevant for subscribers only.
     * </p>
     *
     * @throws org.jlab.coda.xmsg.excp.xMsgException
     */
    public Publisher() throws xMsgException, SocketException {
        super("localhost");
    }

    public static void main(String[] args) {
        try {

            Publisher publisher = new Publisher();

            // Create a socket connections to the xMsg node
            xMsgConnection con =  publisher.connect();

            // Register this publisher
            publisher.registerPublisher(myName, domain, subject,type);

            // Fill payload with random numbers
            Random rg = new Random();
            String topic = xMsgUtil.buildTopic(domain,subject,type);

            // Create the message to be published
            xMsgMessage msg = new xMsgMessage(topic);
            System.out.println("Byte array size = " + args[0]);
            byte[] b = new byte[Integer.parseInt(args[0])];
            msg.setData(b);

            // Publish data for ever...
            while(true) {
                publisher.publish(con, msg);
//                System.out.println("publishing...");
//                xMsgUtil.sleep(1000);

            }

        } catch (xMsgException | IOException e) {
            e.printStackTrace();
        }
    }
}
