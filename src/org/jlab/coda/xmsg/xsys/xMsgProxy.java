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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

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

    public static void main(String[] args) {
        try {
            xMsgProxy proxy = new xMsgProxy();
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

        System.out.println(xMsgUtil.currentTime(4) +
                " Info: Running xMsg proxy server on the localhost..." + "\n");

        // setting up the xMsg proxy
        // socket where clients publish their data/messages
        ZMQ.Socket in = context.createSocket(ZMQ.XSUB);
        in.bind("tcp://*:" + xMsgConstants.DEFAULT_PORT.getIntValue());

        // socket where clients subscribe data/messages
        ZMQ.Socket out = context.createSocket(ZMQ.XPUB);
        out.bind("tcp://*:" + (xMsgConstants.DEFAULT_PORT.getIntValue() + 1));

        // start poxy. this will block for ever
        ZMQ.proxy(in, out, null);
    }
}