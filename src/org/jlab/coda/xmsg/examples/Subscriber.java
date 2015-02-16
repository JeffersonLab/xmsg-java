package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;

import java.net.SocketException;

/**
 * xMsg subscriber that checks the local registration to find
 * out if there is a publisher publishing to a specified
 * domain, subject and type. In case publisher is found subscribes
 * the published data. It also includes the inner class
 * presenting the callback to be executed at every arrival od the
 * data. Callback does simple print of the received message.
 *
 * @author gurjyan
 * @version 4.x
 * @since 11/4/14
 */
public class Subscriber extends xMsg {
    private static final String myName = "test_subscriber";
    private static final String domain = "test_domain";
    private static final String subject = "test_subject";
    private static final String type = "test_type";
    private static final String description = "test_description";
    private MyCallBack callback;

    public Subscriber() throws xMsgException, SocketException {
        super("localhost");
        callback = new MyCallBack();
    }

    public static void main(String[] args) {
        try {
            Subscriber subscriber = new Subscriber();

            // Create a socket connections to the xMsg node
            xMsgConnection con =  subscriber.connect();

            // Register this subscriber
            subscriber.registerSubscriber(myName, domain, subject, type, description);

            // Find a publisher that publishes to requested topic
            // defined as a static variables above
            if (subscriber.isThereLocalPublisher(myName, domain, subject, type)){

                // Subscribe by passing a callback to the subscription
                subscriber.subscribe(con, domain, subject, type, subscriber.callback, true);

                xMsgUtil.keepAlive();

            }
        } catch (xMsgException | SocketException e) {
            e.printStackTrace();
        }
    }

    private class MyCallBack implements xMsgCallBack{

        @Override
        public Object callback(xMsgMessage msg) {
            System.out.println(msg);
            return msg.getData();
        }
    }
}

