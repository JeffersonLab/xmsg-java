package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.TimeoutException;

public class SyncPublisher extends xMsg {
    private static final String myName = "test_publisher";
    private static final String domain = "test_domain";
    private static final String subject = "test_subject";
    private static final String type = "test_type";

    public SyncPublisher() throws xMsgException, SocketException {
        super("localhost");
    }

    public static void main(String[] args) {
        try {

            SyncPublisher publisher = new SyncPublisher();

            xMsgConnection con =  publisher.connect();

            publisher.registerPublisher(myName, domain, subject,type);
            String topic = xMsgUtil.buildTopic(domain,subject,type);

            xMsgMessage msg = new xMsgMessage(topic);
            msg.setData(111);
            int counter = 1;
            Object recData;
            while (true) {
                System.out.println("Publishing " + counter);
                long t1 = System.nanoTime();
                recData = publisher.sync_publish(con, msg, 5);
                long t2 = System.nanoTime();
                System.out.printf("Received response = %s in %d ms%n", recData, (t2 - t1) / 1000000L);
                counter++;
                msg.setData(String.valueOf(counter));
                xMsgUtil.sleep(2000);
            }
        } catch (xMsgException | TimeoutException | IOException e) {
            e.printStackTrace();
        }
    }
}
