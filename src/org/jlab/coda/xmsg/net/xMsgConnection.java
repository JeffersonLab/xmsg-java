package org.jlab.coda.xmsg.net;

import org.zeromq.ZMQ.Socket;

/**
 * <p>
 *     xMsg connection class. Contains xMSgAddress object and
 *     two zmq socket objects for publishing and subscribing
 *     xMsg messages respectfully.
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgConnection {

    private xMsgAddress address;
    private Socket pubSock;
    private Socket subSock;


    public xMsgAddress getAddress() {
        return address;
    }

    public void setAddress(xMsgAddress address) {
        this.address = address;
    }

    public Socket getPubSock() {
        return pubSock;
    }

    public void setPubSock(Socket pubSock) {
        this.pubSock = pubSock;
    }

    public Socket getSubSock() {
        return subSock;
    }

    public void setSubSock(Socket subSock) {
        this.subSock = subSock;
    }
}
