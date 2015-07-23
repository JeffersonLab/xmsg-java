/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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

import org.jlab.coda.xmsg.core.xMsgContext;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZThread;
import org.zeromq.ZThread.IAttachedRunnable;

/**
 * xMsg pub-sub proxy executable.
 * This is a simple stateless message switch, i.e. a device that forwards
 * messages without inspecting them. This simplifies dynamic discovery problem.
 * All xMsg actors (publishers and subscribers) connect to the proxy, instead
 * of to each other. It becomes trivial to add more subscribers or publishers.
 *
 * @author gurjyan
 * @version 2.x
 * @since 5/5/15
 */
public class xMsgProxy {

    private final xMsgProxyAddress addr;
    private final ZContext ctx;
    private final ZMQ.Socket in;
    private final ZMQ.Socket out;

    private boolean verbose = false;

    public static void main(String[] args) {
        try {
            xMsgProxyAddress address = new xMsgProxyAddress();
            if (args.length == 2) {
                if (args[0].equals("-port")) {
                    int port = Integer.parseInt(args[1]);
                    if (port <= 0) {
                        System.err.println("Invalid port: " + port);
                        System.exit(1);
                    }
                    address = new xMsgProxyAddress("localhost", port);
                } else {
                    System.err.println("Wrong option. Accepts -port option only.");
                    System.exit(1);
                }
            }

            xMsgProxy proxy = new xMsgProxy(xMsgContext.getContext(), address);
            proxy.start();

        } catch (xMsgException | NumberFormatException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Construct the proxy with the given local address.
     *
     * @param context zmq context object
     * @param address the local address
     */
    public xMsgProxy(ZContext context, xMsgProxyAddress address) {
        ctx = context;
        addr = address;

        // socket where clients publish their data/messages
        in = context.createSocket(ZMQ.XSUB);
        in.setRcvHWM(0);
        in.setSndHWM(0);
        in.bind("tcp://*:" + address.port());

        // socket where clients subscribe data/messages
        out = context.createSocket(ZMQ.XPUB);
        out.setRcvHWM(0);
        out.setSndHWM(0);
        out.bind("tcp://*:" + (address.port() + 1));
    }

    /**
     * Construct the proxy on a local host.
     *
     * @param context zmq context object
     */
    public xMsgProxy(ZContext context) {
        this(context, new xMsgProxyAddress());
    }

    /**
     * Prints every received message.
     */
    public void verbose() {
        this.verbose = true;
    }

    /**
     * Starts the proxy.
     *
     * @throws xMsgException if the proxy could not be started
     */
    public void start() throws xMsgException {

        try {
            System.out.println(xMsgUtil.currentTime(4) +
                    " xMsg-Info: Running xMsg proxy on the host = " +
                    addr.host() + " port = " + addr.port() + "\n");

            // start proxy. this will block for ever
            if (verbose) {
                Socket listener = ZThread.fork(ctx, new Listener());
                ZMQ.proxy(in, out, listener);
            } else {
                ZMQ.proxy(in, out, null);
            }
        } catch (ZMQException e) {
            throw new xMsgException(e.getMessage());
        }
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
}
