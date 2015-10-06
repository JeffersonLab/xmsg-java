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
import org.jlab.coda.xmsg.core.xMsgContext;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegService;
import org.zeromq.ZContext;

import java.io.IOException;

/**
 * xMsgRegistrar executable. Starts a local registrar service in it's own thread.
 *
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgRegistrar {

    private final Thread regServiceThread;
    private final ZContext context = xMsgContext.getContext();

    /**
     * Constructs a registrar that uses the default
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT port}.
     *
     * @throws IOException
     *
     */
    public xMsgRegistrar() throws IOException {
        this(xMsgConstants.REGISTRAR_PORT.getIntValue());
    }

    /**
     * Constructs a registrar that uses the specified port number.
     *
     * @param port registrar port number
     * @throws IOException
     */
    public xMsgRegistrar(int port) throws IOException {

        ZContext shadowContext = ZContext.shadow(context);

        // create registrar service object
        xMsgRegService regService = new xMsgRegService(shadowContext,
                xMsgUtil.localhost(), port);

        // create a new thread that will satisfy registration and discovery requests
        // using created registrar runnable object
        regServiceThread = xMsgUtil.newThread("registration-service", regService);
    }


    public static void main(String[] args) {
        try {
            int port = xMsgConstants.REGISTRAR_PORT.getIntValue();
            if (args.length == 2) {
                if (args[0].equals("-port")) {
                    port = Integer.parseInt(args[1]);
                    if (port <= 0) {
                        System.err.println("Invalid port: " + port);
                        System.exit(1);
                    }
                } else {
                    System.err.println("Wrong option. Accepts -port option only.");
                    System.exit(1);
                }
            }

            final xMsgRegistrar registrar = new xMsgRegistrar(port);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    registrar.shutdown();
                }
            });

            // start the thread to service the requests
            registrar.start();

        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.out.println("exiting...");
            System.exit(1);
        }
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
