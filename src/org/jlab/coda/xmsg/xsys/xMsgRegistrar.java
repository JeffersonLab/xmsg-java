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
 * xMsgRegistrar.
 * Note that no arg constructed object can play master registrar role.
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgRegistrar {

    private final Thread regServiceThread;
    private final ZContext context = xMsgContext.getContext();

    /**
     * Starts a local registrar service.
     * Note: this version assumes that xMsgNode and xMsgFE registrar services
     * use default registrar port:
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * @throws IOException
     *
     */
    public xMsgRegistrar() throws IOException {

        ZContext shadowContext = ZContext.shadow(context);

        // start registrar service
        xMsgRegService regService = new xMsgRegService(shadowContext,
                xMsgUtil.localhost(), xMsgConstants.REGISTRAR_PORT.getIntValue());
        regServiceThread = xMsgUtil.newThread("registration-service", regService);
    }

    public xMsgRegistrar(int port) throws IOException {

        ZContext shadowContext = ZContext.shadow(context);

        // start registrar service
        xMsgRegService regService = new xMsgRegService(shadowContext,
                xMsgUtil.localhost(), port);
        regServiceThread = xMsgUtil.newThread("registration-service", regService);
    }


    public static void main(String[] args) {
        try {
            int port = 0;
            if (args.length == 2) {
                if (args[0].equals("-port")) {
                    port = Integer.parseInt(args[1]);
                } else {
                    System.err.println("Wrong option. Accepts -port option only.");
                    System.exit(1);
                }
            }


            final xMsgRegistrar registrar;
            if (port <= 0) {
                registrar = new xMsgRegistrar();
            } else {
                registrar = new xMsgRegistrar(port);
            }

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    registrar.shutdown();
                }
            });

            registrar.start();

        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.out.println("exiting...");
            System.exit(1);
        }
    }

    public void start() {
        regServiceThread.start();
    }

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
