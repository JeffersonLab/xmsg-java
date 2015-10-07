package org.jlab.coda.xmsg.net;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class xMsgConnectionFactory {

    private ZContext context;

    public xMsgConnectionFactory(ZContext context) {
        this.context = context;

        // fix default linger
        this.context.setLinger(-1);
    }

    public xMsgConnection createProxyConnection(xMsgAddress address,
                                                xMsgConnectionSetup setup) {
        Socket pubSock = context.createSocket(ZMQ.PUB);
        Socket subSock = context.createSocket(ZMQ.SUB);
        setup.preConnection(pubSock);
        setup.preConnection(subSock);

        int pubPort = address.getPort();
        int subPort = pubPort + 1;
        pubSock.connect("tcp://" + address.getHost() + ":" + pubPort);
        subSock.connect("tcp://" + address.getHost() + ":" + subPort);
        setup.postConnection();

        xMsgConnection connection = new xMsgConnection();
        connection.setAddress(address);
        connection.setPubSock(pubSock);
        connection.setSubSock(subSock);

        return connection;
    }

    public void destroyProxyConnection(xMsgConnection connection) {
        context.destroySocket(connection.getPubSock());
        context.destroySocket(connection.getSubSock());
    }

    public void setLinger(int linger) {
        context.setLinger(linger);
    }

    public void destroy() {
        context.destroy();
    }
}
