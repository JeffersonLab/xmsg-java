package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.TimeoutException;

public class SyncPublisher extends xMsg {

    public SyncPublisher() throws xMsgException, SocketException {
        super("test_sync_publisher", "localhost");
    }

    public static void main(String[] args) {
        try {
            final String domain = "test_domain";
            final String subject = "test_subject";
            final String type = "test_type";
            final String description = "test_description";

            SyncPublisher publisher = new SyncPublisher();

            xMsgConnection con =  publisher.connect();
            xMsgTopic topic = xMsgTopic.build(domain, subject, type);

            publisher.registerPublisher(topic, description);

            xMsgMessage msg = new xMsgMessage(topic);
            setData(msg, 111);
            int counter = 1;
            while (true) {
                System.out.println("Publishing " + counter);
                long t1 = System.nanoTime();
                Object recData = publisher.syncPublish(con, msg, 5);
                long t2 = System.nanoTime();
                long delta = (t2 - t1) / 1000000L;
                System.out.printf("Received response = %s in %d ms%n", recData, delta);
                counter++;
                setData(msg, counter);
                xMsgUtil.sleep(2000);
            }
        } catch (xMsgException | TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }


    private static void setData(xMsgMessage msg, int value) {
        xMsgData.Builder b = xMsgData.newBuilder();
        b.setFLSINT32(value);
        msg.setData(b.build());
    }
}
