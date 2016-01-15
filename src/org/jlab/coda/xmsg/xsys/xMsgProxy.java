/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.coda.xmsg.xsys;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZThread;
import org.zeromq.ZThread.IAttachedRunnable;

/**
 * Runs xMsg pub-sub proxy.
 * This is a simple stateless message switch, i.e. a device that forwards
 * messages without inspecting them. This simplifies dynamic discovery problem.
 * All xMsg clients (publishers and subscribers) connect to the proxy, instead
 * of to each other. It becomes trivial to add more subscribers or publishers.
 *
 * @author gurjyan
 * @version 2.x
 * @since 5/5/15
 */
public class xMsgProxy {

    private boolean verbose = false;

    public static void main(String[] args) {
        try {
            xMsgProxy proxy = new xMsgProxy();
            if (System.getenv("XMSG_PROXY_DEBUG") != null) {
                proxy.verbose();
            }
            proxy.startProxy(new ZContext());

        } catch (xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Starts the proxy server of the xMsgNode on a local host.
     *
     * @param context zmq context object
     * @throws xMsgException
     */
    public void startProxy(ZContext context) throws xMsgException {
        xMsgAddress address = new xMsgAddress(localhost());

        System.out.println(xMsgUtil.currentTime(4) +
                " Info: Running xMsg proxy server on the localhost..." + "\n");

        // setting up the xMsg proxy
        // socket where clients publish their data/messages
        Socket in = createSocket(context, ZMQ.XSUB);
        bindSocket(in, address.getPort());

        // socket where clients subscribe data/messages
        Socket out = createSocket(context, ZMQ.XPUB);
        bindSocket(out, address.getPort() + 1);

        // start proxy. this will block for ever
        if (verbose) {
            Socket listener = ZThread.fork(context, new Listener());
            ZMQ.proxy(in, out, listener);
        } else {
            ZMQ.proxy(in, out, null);
        }
    }


    /**
     * Prints every received message.
     */
    public void verbose() {
        this.verbose = true;
    }


    /**
     * The listener receives all messages flowing through the proxy,
     * on its pipe.
     */
    private static class Listener implements IAttachedRunnable {
        @Override
        public void run(Object[] args, ZContext ctx, Socket pipe) {
            //  Print everything that arrives on pipe
            while (true) {
                ZMsg msg = ZMsg.recvMsg(pipe);
                if (msg == null) {
                    System.out.println("Interrupted...");
                    break;
                }
                msg.pop().print(null);
                msg.destroy();
            }
        }
    }


    private static String localhost() {
        try {
            return xMsgUtil.localhost();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    private static Socket createSocket(ZContext ctx, int type) {
        Socket socket = ctx.createSocket(type);
        socket.setRcvHWM(0);
        socket.setSndHWM(0);
        return socket;
    }


    private static void bindSocket(Socket socket, int port) {
        socket.bind("tcp://*:" + port);
    }


    private static void connectSocket(Socket socket, String host, int port) {
        socket.connect("tcp://" + host + ":" + port);
    }
}
