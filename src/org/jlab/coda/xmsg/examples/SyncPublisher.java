package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SyncPublisher extends xMsg {

    public SyncPublisher() throws IOException {
        super("test_sync_publisher");
    }

    public static void main(String[] args) {
        try {
            final String domain = "test_domain";
            final String subject = "test_subject";
            final String type = "test_type";
            final String description = "test_description";

            SyncPublisher publisher = new SyncPublisher();

            xMsgTopic topic = xMsgTopic.build(domain, subject, type);

            publisher.registerAsPublisher(topic, description);

            int counter = 1;
            while (true) {
                System.out.println("Publishing " + counter);
                long t1 = System.nanoTime();
                Object recData = publisher.syncPublish(topic, 111, 5);
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
