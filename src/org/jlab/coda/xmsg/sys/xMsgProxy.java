/*
 *    Copyright (C) 2016. Jefferson Lab (JLAB). All Rights Reserved.
 *    Permission to use, copy, modify, and distribute this software and its
 *    documentation for governmental use, educational, research, and not-for-profit
 *    purposes, without fee and without a signed licensing agreement.
 *
 *    IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 *    INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 *    THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 *    OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *    JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *    THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *    PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 *    HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 *    SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *    This software was developed under the United States Government License.
 *    For more information contact author at gurjyan@jlab.org
 *    Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.coda.xmsg.sys;

import static java.util.Arrays.asList;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.jlab.coda.xmsg.sys.pubsub.xMsgCtrlConstants;
import org.jlab.coda.xmsg.sys.util.Environment;
import org.jlab.coda.xmsg.sys.util.LogUtils;
import org.jlab.coda.xmsg.sys.util.ThreadUtils;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
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
 * xMsg pub-sub proxy server.
 * This is a simple stateless message switch, i.e. a device that forwards
 * messages without inspecting them.
 * <p>
 * The proxy simplifies the dynamic connection problems. All xMsg actors
 * (publishers and subscribers) connect to the proxy, instead of to each other.
 * It becomes trivial to add more subscribers or publishers.
 *
 * @version 2.x
 */
public class xMsgProxy {

    private final xMsgProxyAddress addr;
    private final xMsgContext ctx;

    private final Thread proxy;
    private final Thread controller;

    private static final Logger LOGGER = LogUtils.getConsoleLogger("xMsgProxy");

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

            LOGGER.setLevel(Level.INFO);

            String host = options.valueOf(hostSpec);
            int port = options.valueOf(portSpec);
            xMsgProxyAddress address = new xMsgProxyAddress(host, port);

            xMsgProxy proxy = new xMsgProxy(xMsgContext.getInstance(), address);
            if (options.has("verbose")) {
                proxy.verbose();
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                xMsgContext.getInstance().destroy();
                proxy.shutdown();
            }));

            proxy.start();

        } catch (OptionException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
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
     * Construct a proxy that uses the localhost and
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#DEFAULT_PORT default port}.
     *
     * @param context the context to handle the proxy sockets
     * @throws xMsgException if the address is already in use
     */
    public xMsgProxy(xMsgContext context) throws xMsgException {
        this(context, new xMsgProxyAddress());
    }

    /**
     * Construct the proxy with the given local address.
     *
     * @param context the context to handle the proxy sockets
     * @param address the local address
     * @throws xMsgException if the address is already in use
     */
    public xMsgProxy(xMsgContext context, xMsgProxyAddress address) throws xMsgException {
        ctx = context;
        addr = address;

        Proxy proxyTask = null;
        Controller controllerTask = null;
        try {
            proxyTask = new Proxy();
            controllerTask = new Controller();
            proxy = ThreadUtils.newThread("proxy", proxyTask);
            controller = ThreadUtils.newThread("control", controllerTask);
        } catch (Exception e) {
            if (proxyTask != null) {
                proxyTask.close();
            }
            if (controllerTask != null) {
                controllerTask.close();
            }
            throw e;
        }

        if (Environment.isDefined("XMSG_PROXY_DEBUG")) {
            verbose();
        }
    }

    /**
     * Prints every received message.
     */
    public void verbose() {
        LOGGER.setLevel(Level.FINE);
    }

    /**
     * Starts the proxy.
     */
    public void start() {
        proxy.start();
        controller.start();
    }

    /**
     * Stops the proxy. The context must be destroyed first.
     */
    public void shutdown() {
        try {
            proxy.interrupt();
            controller.interrupt();
            proxy.join();
            controller.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tests if the proxy is running.
     *
     * @return true if the proxy is running, false otherwise
     */
    public boolean isAlive() {
        return proxy.isAlive() && controller.isAlive();
    }

    /**
     * Returns the address of the proxy.
     *
     * @return the address used by the proxy
     */
    public xMsgProxyAddress address() {
        return addr;
    }


    /**
     * The proxy forwards pub/sub communications.
     */
    private class Proxy implements Runnable {

        final Socket in;
        final Socket out;

        final xMsgSocketFactory factory = new xMsgSocketFactory(ctx.getContext());

        Proxy() throws xMsgException {
            Socket in = null;
            Socket out = null;
            try {
                in = factory.createSocket(ZMQ.XSUB);
                out = factory.createSocket(ZMQ.XPUB);
                factory.bindSocket(in, addr.pubPort());
                factory.bindSocket(out, addr.subPort());
            } catch (Exception e) {
                factory.closeQuietly(in);
                factory.closeQuietly(out);
                throw e;
            }
            this.in = in;
            this.out = out;
        }

        @Override
        public void run() {
            ZContext wrapper = wrapContext();
            try {
                LOGGER.info("running on host = " + addr.host() + "  port = " + addr.pubPort());
                if (LOGGER.isLoggable(Level.FINE)) {
                    Socket listener = ZThread.fork(wrapper, new Listener());
                    ZMQ.proxy(in, out, listener);
                } else {
                    ZMQ.proxy(in, out, null);
                }
            } catch (Exception e) {
                LOGGER.severe(LogUtils.exceptionReporter(e));
            }  finally {
                wrapper.close();
                close();
            }
        }

        private ZContext wrapContext() {
            try {
                // I don't want to expose the ZContext private field in xMsgContext
                // since it is never actually used except here.
                Field zctx = ctx.getClass().getDeclaredField("ctx");
                zctx.setAccessible(true);
                return ZContext.shadow((ZContext) zctx.get(ctx));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            factory.closeQuietly(in);
            factory.closeQuietly(out);
        }
    }


    /**
     * The controller receives and replies synchronization control messages from
     * connections.
     */
    private class Controller implements Runnable {

        final Socket control;
        final Socket publisher;
        final Socket router;

        final xMsgSocketFactory factory = new xMsgSocketFactory(ctx.getContext());

        Controller() throws xMsgException {
            Socket control = null;
            Socket publisher = null;
            Socket router = null;
            try {
                control = factory.createSocket(ZMQ.SUB);
                publisher = factory.createSocket(ZMQ.PUB);
                router = factory.createSocket(ZMQ.ROUTER);

                factory.connectSocket(control, addr.host(), addr.subPort());
                factory.connectSocket(publisher, addr.host(), addr.pubPort());

                router.setRouterHandover(true);
                factory.bindSocket(router, addr.pubPort() + 2);

                control.subscribe(xMsgCtrlConstants.CTRL_TOPIC.getBytes());
            } catch (Exception e) {
                factory.closeQuietly(control);
                factory.closeQuietly(publisher);
                factory.closeQuietly(router);
                throw e;
            }
            this.control = control;
            this.publisher = publisher;
            this.router = router;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        ZMsg msg = ZMsg.recvMsg(control);
                        if (msg == null) {
                            break;
                        }
                        processRequet(msg);
                    } catch (ZMQException e) {
                        if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                            break;
                        }
                        LOGGER.warning(LogUtils.exceptionReporter(e));
                    }
                }
            } catch (Exception e) {
                LOGGER.severe(LogUtils.exceptionReporter(e));
            } finally {
                close();
            }
        }

        private void processRequet(ZMsg msg) {
            /* ZFrame topicFrame = */ msg.pop();
            ZFrame typeFrame = msg.pop();
            ZFrame idFrame = msg.pop();

            String type = new String(typeFrame.getData());
            String id = new String(idFrame.getData());

            switch (type) {
                case xMsgCtrlConstants.CTRL_CONNECT: {
                    ZMsg ack = new ZMsg();
                    ack.add(id);
                    ack.add(type);
                    ack.send(router);
                    break;
                }
                case xMsgCtrlConstants.CTRL_SUBSCRIBE: {
                    ZMsg ack = new ZMsg();
                    ack.add(id);
                    ack.add(type);
                    ack.send(publisher);
                    break;
                }
                case xMsgCtrlConstants.CTRL_REPLY: {
                    ZMsg ack = new ZMsg();
                    ack.add(id);
                    ack.add(type);
                    ack.send(router);
                    break;
                }
                default:
                    LOGGER.warning("unexpected request: " + type);
            }
        }

        public void close() {
            factory.closeQuietly(control);
            factory.closeQuietly(publisher);
            factory.closeQuietly(router);
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
                try {
                    ZMsg msg = ZMsg.recvMsg(pipe);
                    if (msg == null) {
                        break;
                    }
                    ZFrame frame = msg.pop();
                    byte[] data = frame.getData();
                    if (data[0] == 1) {
                        String topic = new String(data, 1, data.length - 1);
                        LOGGER.fine("subscribed topic = " + topic);
                    } else if (data[0] == 0) {
                        String topic = new String(data, 1, data.length - 1);
                        LOGGER.fine("unsubscribed topic = " + topic);
                    } else {
                        LOGGER.fine("received topic = " + frame.toString());
                    }
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                        break;
                    }
                    LOGGER.warning(LogUtils.exceptionReporter(e));
                } catch (Exception e) {
                    LOGGER.severe(LogUtils.exceptionReporter(e));
                    break;
                }
            }
        }
    }
}
