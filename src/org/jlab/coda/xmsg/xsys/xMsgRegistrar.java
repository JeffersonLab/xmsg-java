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

import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegService;
import org.zeromq.ZContext;

import java.net.SocketException;

/**
 * xMsgRegistrar.
 * Note that no arg constructed object can play master registrar role.
 *
 * @author gurjyan
 * @since 1.0
 */
public class xMsgRegistrar extends xMsgRegDriver {

    private final Thread regServiceThread;
    private final ZContext context;

    /**
     * Starts a local registrar service.
     * Note: this version assumes that xMsgNode and xMsgFE registrar services
     * use default registrar port:
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     *
     * @throws SocketException
     * @throws xMsgException
     */
    public xMsgRegistrar() throws SocketException, xMsgException {

        super("localhost");

        context = getContext();
        ZContext shadowContext = ZContext.shadow(context);

        // start registrar service
        xMsgRegService regService = new xMsgRegService(shadowContext);
        regServiceThread = xMsgUtil.newThread("registration-service", regService);
    }


    /**
     * Starts a local registrar service.
     * Constructor of the {@link xMsgRegService} class will start a
     * thread that will periodically report local registration database to
     * xMsgRegistrar service that is defined to be a master Registrar service
     * (FE).
     * This way registration data is distributed/duplicated between xMsgNode and
     * xMsgFE registrar services.
     * That is the reason we need to pass xMsg front-end host name.
     * <p>
     * Note: this version assumes that xMsgNode and xMsgFE registrar services
     * use default registrar port:
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     *
     * @param feHost xMsg front-end host. Host is passed through command line -h option,
     *               or through the environmental variable: XMSG_FE_HOST
     * @throws SocketException
     * @throws xMsgException
     */
    public xMsgRegistrar(final String feHost) throws SocketException, xMsgException {

        super(feHost);

        // Zmq context
        context = getContext();
        ZContext shadowContext = ZContext.shadow(context);

        // Local registrar service.
        // In this case this specific constructor starts a thread
        // that periodically updates front-end registrar database with
        // the data from the local databases
        xMsgRegService regService = new xMsgRegService(feHost, shadowContext);
        regServiceThread = xMsgUtil.newThread("registration-service", regService);
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


    public static void main(String[] args) {
        try {
            String localHost = xMsgUtil.toHostAddress("localhost");
            String frontEndHost = localHost;
            if (args.length == 2) {
                if (args[0].equals("-fe_host")) {
                    frontEndHost = xMsgUtil.toHostAddress(args[1]);
                } else {
                    System.err.println("Wrong option. Accepts -fe_host option only.");
                    System.exit(1);
                }
            } else if (args.length != 0) {
                System.err.println("Wrong arguments. Accepts -fe_host option only.");
                System.exit(1);
            }


            final xMsgRegistrar registrar;
            if (frontEndHost.equals(localHost)) {
                registrar = new xMsgRegistrar();
            } else {
                registrar = new xMsgRegistrar(frontEndHost);
            }

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    registrar.shutdown();
                }
            });

            registrar.start();

        } catch (xMsgException | SocketException e) {
            System.out.println(e.getMessage());
            System.out.println("exiting...");
            System.exit(1);
        }
    }
}
