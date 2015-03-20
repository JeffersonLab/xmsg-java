package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;

import java.net.SocketException;

public class ReplySubscriber extends xMsg {

    private static final String myName = "test_subscriber";
    private static final String domain = "test_domain";
    private static final String subject = "test_subject";
    private static final String type = "test_type";
    private static final String description = "test_description";

    private MyCallBack callback;
    private xMsgConnection connection;


    public static void main(String[] args) {
        try {
            ReplySubscriber subscriber = new ReplySubscriber();
            subscriber.registerSubscriber(myName, domain, subject, type, description);
            subscriber.subscribe(subscriber.connection, domain, subject, type, subscriber.callback);
        } catch (xMsgException | SocketException e) {
            e.printStackTrace();
        }
    }


    public ReplySubscriber() throws xMsgException, SocketException {
        super("localhost");
        connection = connect();
        callback = new MyCallBack();
    }

    private class MyCallBack implements xMsgCallBack {

        @Override
        public Object callback(xMsgMessage msg) {
            // processing goes here
            String result = xMsgConstants.DONE.getStringValue();

            if(msg.getIsSyncRequest()) {
                // sync request
                String syncReceiver = msg.getSyncRequesterAddress();
                try {
                    publish(connection, syncReceiver, result);
                    System.out.println("Published response to " + syncReceiver);
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
                return result;
            }
        }
    }

