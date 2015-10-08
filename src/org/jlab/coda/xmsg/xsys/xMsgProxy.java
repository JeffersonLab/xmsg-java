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

            xMsgProxy proxy = new xMsgProxy();
            proxy.startProxy(xMsgContext.getContext(), address);

        } catch (xMsgException | NumberFormatException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Starts the proxy on a local host, with specified port.
     *
     * @param context zmq context object
     * @throws xMsgException
     */
    public void startProxy(ZContext context) throws xMsgException {
        startProxy(context, new xMsgProxyAddress());
    }

    /**
     * Starts the proxy on a local host, with specified port.
     *
     * @param context zmq context object
     * @throws xMsgException
     */
    public void startProxy(ZContext context, xMsgProxyAddress address) throws xMsgException {

        System.out.println(xMsgUtil.currentTime(4) +
                " xMsg-Info: Running xMsg proxy on the host = " +
                address.host() + " port = " + address.port() + "\n");

        // setting up the xMsg proxy
        // socket where clients publish their data/messages
        ZMQ.Socket in = context.createSocket(ZMQ.XSUB);
        in.setRcvHWM(0);
        in.setSndHWM(0);
        in.bind("tcp://*:" + address.port());

        // socket where clients subscribe data/messages
        ZMQ.Socket out = context.createSocket(ZMQ.XPUB);
        out.setRcvHWM(0);
        out.setSndHWM(0);
        out.bind("tcp://*:" + (address.port() + 1));

        // start poxy. this will block for ever
        ZMQ.proxy(in, out, null);
    }
}
