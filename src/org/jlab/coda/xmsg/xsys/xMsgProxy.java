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

import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.PrintStream;

import org.jlab.coda.xmsg.core.xMsgConstants;
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

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

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

    private boolean verbose = false;

    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();
            OptionSpec<String> hostSpec = parser.accepts("host")
                    .withRequiredArg()
                    .defaultsTo(xMsgUtil.localhost());
            OptionSpec<Integer> portSpec = parser.accepts("port")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(xMsgConstants.DEFAULT_PORT);
            parser.accepts("verbose");
            parser.acceptsAll(asList("h", "help")).forHelp();
            OptionSet options = parser.parse(args);

            if (options.has("help")) {
                usage(System.out);
                System.exit(0);
            }

            String host = options.valueOf(hostSpec);
            int port = options.valueOf(portSpec);
            xMsgProxyAddress address = new xMsgProxyAddress(host, port);

            xMsgProxy proxy = new xMsgProxy(xMsgContext.getContext(), address);
            if (options.has("verbose")) {
                proxy.verbose();
            }
            proxy.start();

        } catch (OptionException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException | xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void usage(PrintStream out) {
        out.printf("usage: jx_proxy [options]%n%n  Options:%n");
        out.printf("  %-22s  %s%n", "-host <hostname>", "use the given hostname");
        out.printf("  %-22s  %s%n", "-port <port>", "use the given port");
        out.printf("  %-22s  %s%n", "-verbose", "print debug information");
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
            // socket where clients publish their data/messages
            ZMQ.Socket in = createSocket(ctx, ZMQ.XSUB);
            bindSocket(in, addr.port());

            // socket where clients subscribe data/messages
            ZMQ.Socket out = createSocket(ctx, ZMQ.XPUB);
            bindSocket(out, addr.port() + 1);

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
