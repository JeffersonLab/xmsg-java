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

package org.jlab.coda.xmsg.xsys;

import static java.util.Arrays.asList;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegService;
import org.zeromq.ZContext;

import java.io.PrintStream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * xMsgRegistrar executable. Starts a local registrar service in it's own thread.
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgRegistrar {

    private final Thread regServiceThread;
    private final ZContext context;

    /**
     * Constructs a registrar that uses the default
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT port}.
     *
     * @param context the context to run the registrar service
     */
    public xMsgRegistrar(ZContext context) {
        this(context, new xMsgRegAddress());
    }

    /**
     * Constructs a registrar that uses the specified port number.
     *
     * @param context the context to run the registrar service
     * @param address the address of the registrar service
     */
    public xMsgRegistrar(ZContext context, xMsgRegAddress address) {

        this.context = context;
        ZContext shadowContext = ZContext.shadow(context);

        // create registrar service object
        xMsgRegService regService = new xMsgRegService(shadowContext, address);

        // create a new thread that will satisfy registration and discovery requests
        // using created registrar runnable object
        regServiceThread = xMsgUtil.newThread("registration-service", regService);
    }


    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();
            OptionSpec<Integer> portSpec = parser.accepts("port")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(xMsgConstants.REGISTRAR_PORT);
            parser.acceptsAll(asList("h", "help")).forHelp();
            OptionSet options = parser.parse(args);

            if (options.has("help")) {
                usage(System.out);
                System.exit(0);
            }

            int port = options.valueOf(portSpec);
            xMsgRegAddress address = new xMsgRegAddress("localhost", port);

            final ZContext context = xMsgContext.getContext();
            final xMsgRegistrar registrar = new xMsgRegistrar(context, address);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    registrar.shutdown();
                }
            });

            // start the thread to service the requests
            registrar.start();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("exiting...");
            System.exit(1);
        }
    }

    private static void usage(PrintStream out) {
        out.printf("usage: jx_registrar [ -port <port> ]%n");
    }

    /**
     * Starts the registration and discovery servicing thread.
     */
    public void start() {
        regServiceThread.start();
    }

    /**
     * Stops the registration and discovery service.
     */
    public void shutdown() {
        try {
            context.destroy();
            regServiceThread.interrupt();
            regServiceThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
