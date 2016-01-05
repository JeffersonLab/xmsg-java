package org.jlab.coda.xmsg.net;

import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
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

    public xMsgConnection createProxyConnection(xMsgProxyAddress address,
                                                xMsgConnectionSetup setup) {
        Socket pubSock = context.createSocket(ZMQ.PUB);
        Socket subSock = context.createSocket(ZMQ.SUB);
        setup.preConnection(pubSock);
        setup.preConnection(subSock);

        int pubPort = address.port();
        int subPort = pubPort + 1;
        pubSock.connect("tcp://" + address.host() + ":" + pubPort);
        subSock.connect("tcp://" + address.host() + ":" + subPort);
        setup.postConnection();

        xMsgConnection connection = new xMsgConnection();
        connection.setAddress(address);
        connection.setPubSock(pubSock);
        connection.setSubSock(subSock);

        return connection;
    }

    public xMsgRegDriver createRegistrarConnection(xMsgRegAddress address) {
        return new xMsgRegDriver(context, address);
    }

    public void destroyProxyConnection(xMsgConnection connection) {
        context.destroySocket(connection.getPubSock());
        context.destroySocket(connection.getSubSock());
    }

    public void destroyRegistrarConnection(xMsgRegDriver connection) {
        connection.destroy();
    }

    public void setLinger(int linger) {
        context.setLinger(linger);
    }

    public void destroy() {
        if (!context.isMain()) {
            context.destroy();
        }
    }
}
