package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

/**
 * <p>
 *     xMsg subscription handler
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 3/6/15
 */

public abstract class SubscriptionHandler implements Runnable {

    public abstract void handle() throws xMsgException;

    private boolean isRunning = true;

    private Socket con;
    private String topic;

    public SubscriptionHandler(xMsgConnection connection,
                               String topic){
        con = connection.getSubSock();
        this.topic = topic;
    }

    @Override
    public void run() {
        while(isRunning){
            try {
                handle();
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        }
        con.unsubscribe(topic.getBytes(ZMQ.CHARSET));
    }

    public void unsubscribe(){
        isRunning = false;
    }

}
