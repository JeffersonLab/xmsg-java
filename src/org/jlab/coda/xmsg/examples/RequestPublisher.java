package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;

import java.net.SocketException;
import java.util.concurrent.TimeoutException;

public class RequestPublisher extends xMsg {
    private static final String myName = "test_publisher";
    private static final String domain = "test_domain";
    private static final String subject = "test_subject";
    private static final String type = "test_type";

    public RequestPublisher() throws xMsgException, SocketException {
        super("localhost");
    }

    public static void main(String[] args) {
        try {

            RequestPublisher publisher = new RequestPublisher();
            publisher.registerPublisher(myName, domain, subject,type);

            xMsgConnection con =  publisher.connect();
            xMsgMessage msg = new xMsgMessage(myName, domain, subject,type, String.valueOf(1));

            int counter = 1;
            while (true) {
                System.out.println("Publishing " + counter);
                msg.setData(String.valueOf(counter));
                long t1 = System.nanoTime();
                publisher.sync_publish(con, msg, 5);
                long t2 = System.nanoTime();
                System.out.printf("Received response on %d ms%n", (t2 - t1) / 1000000);
                counter++;
                xMsgUtil.sleep(2000);
            }
        } catch (xMsgException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
