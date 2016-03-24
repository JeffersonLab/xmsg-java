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
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegService;
import org.jlab.coda.xmsg.xsys.util.LogUtils;
import org.zeromq.ZContext;

import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private final xMsgRegAddress addr;
    private final Thread registrar;

    private static final Logger LOGGER = LogUtils.getConsoleLogger("xMsgRegistrar");

    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();
            OptionSpec<Integer> portSpec = parser.accepts("port")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(xMsgConstants.REGISTRAR_PORT);
            parser.accepts("verbose");
            parser.acceptsAll(asList("h", "help")).forHelp();
            OptionSet options = parser.parse(args);

            if (options.has("help")) {
                usage(System.out);
                System.exit(0);
            }

            int port = options.valueOf(portSpec);
            xMsgRegAddress address = new xMsgRegAddress("localhost", port);

            xMsgRegistrar registrar = new xMsgRegistrar(xMsgContext.getContext(), address);

            if (options.has("verbose")) {
                registrar.verbose();
            } else {
                LOGGER.setLevel(Level.INFO);
            }

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    context.destroy();
                    registrar.shutdown();
                }
            });

            registrar.start();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("exiting...");
            System.exit(1);
        }
    }

    private static void usage(PrintStream out) {
        out.printf("usage: jx_registrar [options]%n%n  Options:%n");
        out.printf("  %-22s  %s%n", "-port <port>", "use the given port");
        out.printf("  %-22s  %s%n", "-verbose", "print debug information");
    }

    /**
     * Constructs a registrar that uses the localhost and
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT default port}.
     *
     * @param context the context to run the registrar service
     * @throws xMsgException if the address is already in use
     */
    public xMsgRegistrar(ZContext context) throws xMsgException {
        this(context, new xMsgRegAddress());
    }

    /**
     * Constructs a registrar that uses the specified address.
     *
     * @param context the context to run the registrar service
     * @param address the address of the registrar service
     * @throws xMsgException if the address is already in use
     */
    public xMsgRegistrar(ZContext context, xMsgRegAddress address) throws xMsgException {
        addr = address;
        registrar = xMsgUtil.newThread("registration-service",
                                       new xMsgRegService(context, address));
    }

    /**
     * Prints every received message.
     */
    public void verbose() {
        LOGGER.setLevel(Level.FINE);
    }

    /**
     * Starts the registration and discovery servicing thread.
     */
    public void start() {
        registrar.start();
    }

    /**
     * Stops the registration and discovery service.
     * The context must be destroyed first.
     */
    public void shutdown() {
        try {
            registrar.interrupt();
            registrar.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests if the registrar is running.
     */
    public boolean isAlive() {
        return registrar.isAlive();
    }

    /**
     * Returns the address of the registrar.
     */
    public xMsgRegAddress address() {
        return addr;
    }
}
