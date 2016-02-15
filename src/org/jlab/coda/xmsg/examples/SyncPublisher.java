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
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * An example of a publisher that publishes in sync data for ever.
 * It does not matter who is subscribing to the messages, but subscriber
 * should be able to find out if the received message is sync request
 * amd respond back.
 * This publisher uses the  proxy, running
 * on a localhost, with the default poxy port to publish data.
 * <p/>
 * Published data is hard codded int = 111.
 *
 * @author gurjyan
 * @version 2.x
 */
public class SyncPublisher extends xMsg {

    public xMsgConnection con;
    public xMsgTopic topic;

    /**
     * Calls the parent constructor, connects to the
     * local proxy and creates a topic of conversation,
     * as well as registers with the local registrar.
     *
     * @throws IOException
     */
    public SyncPublisher() throws IOException, xMsgException {
        super("test_sync_publisher");

        // connect to default proxy (local host, default proxy port)
        con = connect();

        // build the publishing topic (hard codded)
        final String domain = "test_domain";
        final String subject = "test_subject";
        final String type = "test_type";
        final String description = "test_description";
        topic = xMsgTopic.build(domain, subject, type);

        // Register this publisher
        registerAsPublisher(topic, description);

    }

    public static void main(String[] args) {
        try {

            SyncPublisher publisher = new SyncPublisher();

            xMsgMessage msg = xMsgMessage.createFrom(publisher.topic, 111);

            int counter = 1;
            while (true) {
                System.out.println("Publishing " + counter);
                long t1 = System.nanoTime();

                //sync publish data. Note this will block for up to 5sec for data to arrive.
                Object recData = publisher.syncPublish(publisher.con, msg, 5000);

                long t2 = System.nanoTime();
                long delta = (t2 - t1) / 1000000L;
                System.out.printf("Received response = %s in %d ms%n", recData, delta);
                counter++;
                xMsgUtil.sleep(2000);
            }
        } catch (xMsgException | TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }
}
