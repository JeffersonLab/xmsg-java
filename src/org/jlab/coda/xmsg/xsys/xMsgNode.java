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
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDiscDriver;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegistrar;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import java.net.SocketException;

/**
 * <p>
 *     xMsgNode.
 *     Runs xMsg pub-sub proxy.
 *     This is a simple stateless message switch, i.e. a device that
 *     forwards messages without inspecting them. This simplifies dynamic
 *     discovery problem. All xMsg clients (publishers and subscribers)
 *     connect to the proxy, instead of to each other. It becomes trivial
 *     to add more subscribers or publishers.
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgNode extends xMsgRegDiscDriver {


    /**
     * <p>
     *     Starts a local registrar service.
     *     Note: this version assumes that xMsgNode and xMsgFE registrar services
     *           use default registrar port:
     *           {@link xMsgConstants#REGISTRAR_PORT}
     * </p>
     * @throws xMsgException
     */
    public xMsgNode() throws xMsgException, SocketException {

        super("localhost");

        // Get local IP addresses in case of multiple network cards.
        // This list is available through xMsgUtil.LOCAL_HOST_IPS
        xMsgUtil.updateLocalHostIps();

        // Zmq context
        final ZContext _context = new ZContext();

        try{
            // Start local registrar service in a separate thread.
            final Thread _registrationThread = new xMsgRegistrar(_context);
            _registrationThread.start();

            System.out.println(xMsgUtil.currentTime(4) +
                    " Info: xMsg local registration and discovery server is started");

            // Shutdown hook, thread that will run jvm before exiting
            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run() {
                    super.run();
                    System.out.println(xMsgUtil.currentTime(4) +
                            " Info: Shutting down xMsg local registration and discovery server");
                    _registrationThread.stop();

                    _context.close();
                    System.out.println("bye...");
                }
            });

            // Starting the xMsg proxy on a local host
            startProxy(_context);

        } catch (xMsgException e) {
            System.out.println(e.getMessage());
            _context.destroy();
        }
    }

    /**
     * <p>
     *     Starts a local registrar service.
     *     Note: this version assumes that xMsgNode and xMsgFE registrar services
     *           use default registrar port:
     *           {@link xMsgConstants#REGISTRAR_PORT}
     * </p>
     * @throws xMsgException
     */
    public xMsgNode(boolean isStandAlone) throws xMsgException, SocketException {

        super("localhost");

        // Get local IP addresses in case of multiple network cards.
        // This list is available through xMsgUtil.LOCAL_HOST_IPS
        xMsgUtil.updateLocalHostIps();

        // Zmq context
        final ZContext _context = new ZContext();

        try{
            // Start local registrar service in a separate thread.
            final Thread _registrationThread = new xMsgRegistrar(_context);
            _registrationThread.start();

            System.out.println(xMsgUtil.currentTime(4) +
                    " Info: xMsg local registration and discovery server is started");

            // Shutdown hook, thread that will run jvm before exiting
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    super.run();
                    System.out.println(xMsgUtil.currentTime(4) +
                            " Info: Shutting down xMsg local registration and discovery server");
                    _registrationThread.stop();

                    _context.close();
                    System.out.println("bye...");
                }
            });

            if(isStandAlone) {
                // Starting the xMsg proxy on a local host
                startProxy(_context);
            } else {
                Thread t1 = new Thread(new Runnable() {
                    public void run() {
                        // Starting the xMsg proxy on a local host
                        try {
                            startProxy(_context);
                        } catch (xMsgException e) {
                            e.printStackTrace();
                        }
                    }
                });
                t1.start();
            }

        } catch (xMsgException e) {
            System.out.println(e.getMessage());
            _context.destroy();
        }
    }

    /**
     * <p>
     *     Starts a local registrar service.
     *     Constructor of the {@link xMsgRegistrar} class
     *     will start a thread that will periodically report local registration
     *     database to xMsgFE ({@link xMsgFE}registrar service.
     *     This ways registration data is distributed/duplicated between xMsgNode
     *     and xMsgFE registrar services.
     *     That is the reason we need to pass xMsg front-end host name.
     *     Note: this version assumes that xMsgNode and xMsgFE registrar services
     *           use default registrar port:
     *           {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * </p>
     * @param feHost xMsg front-end host. Host is passed through command line -h option,
     *               or through the environmental variable: XMSG_FE_HOST
     * @throws xMsgException
     */
    public xMsgNode(final String feHost) throws xMsgException, SocketException {

        super(feHost);

        // Get local IP addresses in case of multiple network cards.
        // This list is available through xMsgUtil.LOCAL_HOST_IPS
        xMsgUtil.updateLocalHostIps();

        // Zmq context
        final ZContext _context = new ZContext();
        try {

            // Start local registrar service in a separate thread.
            // In this case this specific constructor starts a thread
            // that periodically updates front-end registrar database with
            // the data from the local databases
            final Thread _registrationThread = new xMsgRegistrar(feHost, _context);
            _registrationThread.start();

            System.out.println(xMsgUtil.currentTime(4) +
                    " Info: xMsg local registration and discovery server is started");

            // Shutdown hook, thread that will run jvm before exiting
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    super.run();
                    System.out.println(xMsgUtil.currentTime(4) +
                            " Info: Shutting down xMsg local registration and discovery server");
                    _registrationThread.interrupt();

                    // Tell front-end registration server to remove all actors from this host
                    try {
                        removeAllRegistration_fe(feHost, xMsgConstants.UNDEFINED.getStringValue());
                    } catch (xMsgRegistrationException e) {
                        System.out.println(e.getMessage());
                        _context.close();
                        System.out.println("bye...");
                    }
                    _context.close();
                    System.out.println("bye...");
                }
            });

            // Starting the xMsg proxy on a local host
            startProxy(_context);

        } catch (xMsgException e) {
            System.out.println(e.getMessage());
            _context.destroy();
        }
    }

    /**
     * <p>
     *     Starts a local registrar service.
     *     Constructor of the {@link xMsgRegistrar} class
     *     will start a thread that will periodically report local registration
     *     database to xMsgFE ({@link xMsgFE}registrar service.
     *     This ways registration data is distributed/duplicated between xMsgNode
     *     and xMsgFE registrar services.
     *     That is the reason we need to pass xMsg front-end host name.
     *     Note: this version assumes that xMsgNode and xMsgFE registrar services
     *           use default registrar port:
     *           {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * </p>
     * @param feHost xMsg front-end host. Host is passed through command line -h option,
     *               or through the environmental variable: XMSG_FE_HOST
     * @throws xMsgException
     */
    public xMsgNode(final String feHost, Boolean isStandAlone) throws xMsgException, SocketException {

        super(feHost);

        // Get local IP addresses in case of multiple network cards.
        // This list is available through xMsgUtil.LOCAL_HOST_IPS
        xMsgUtil.updateLocalHostIps();

        // Zmq context
        final ZContext _context = new ZContext();
        try {

            // Start local registrar service in a separate thread.
            // In this case this specific constructor starts a thread
            // that periodically updates front-end registrar database with
            // the data from the local databases
            final Thread _registrationThread = new xMsgRegistrar(feHost, _context);
            _registrationThread.start();

            System.out.println(xMsgUtil.currentTime(4) +
                    " Info: xMsg local registration and discovery server is started");

            // Shutdown hook, thread that will run jvm before exiting
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    super.run();
                    System.out.println(xMsgUtil.currentTime(4) +
                            " Info: Shutting down xMsg local registration and discovery server");
                    _registrationThread.interrupt();

                    // Tell front-end registration server to remove all actors from this host
                    try {
                        removeAllRegistration_fe(feHost, xMsgConstants.UNDEFINED.getStringValue());
                    } catch (xMsgRegistrationException e) {
                        System.out.println(e.getMessage());
                        _context.close();
                        System.out.println("bye...");
                    }
                    _context.close();
                    System.out.println("bye...");
                }
            });

            if(isStandAlone) {
                // Starting the xMsg proxy on a local host
                startProxy(_context);
            } else {
                Thread t1 = new Thread(new Runnable() {
                    public void run() {
                        // Starting the xMsg proxy on a local host
                        try {
                            startProxy(_context);
                        } catch (xMsgException e) {
                            e.printStackTrace();
                        }
                    }
                });
                t1.start();
            }

        } catch (xMsgException e) {
            System.out.println(e.getMessage());
            _context.destroy();
        }
    }

    public static void main(String[] args) {

        if(args.length == 2){
            if (args[0].equals("-fe_host")){
                try {
                    new xMsgNode(args[1]);
                } catch (xMsgException e) {
                    System.out.println(e.getMessage());
                    System.out.println("exiting...");
                } catch (SocketException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("wrong option. Accepts -fe_host option only.");
            }
        } else if(args.length == 0){
            try {
                new xMsgNode();
            } catch (xMsgException e) {
                System.out.println(e.getMessage());
                System.out.println("exiting...");
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * <p>
     * Starts the proxy server of the xMsgNode on a local host
     * </p>
     *
     * @param _context zeroMQ context object
     * @throws xMsgException
     */
    private void startProxy(ZContext _context) throws xMsgException {

        System.out.println(xMsgUtil.currentTime(4) +
                " Info: Running xMsg proxy server on the localhost..." + "\n");

        // setting up the xMsg proxy
        // socket where clients publish their data/messages
        Socket in = _context.createSocket(ZMQ.XSUB);
        in.bind("tcp://*:" + xMsgConstants.DEFAULT_PORT.getIntValue());

        // socket where clients subscribe data/messages
        Socket out = _context.createSocket(ZMQ.XPUB);
        out.bind("tcp://*:" + (xMsgConstants.DEFAULT_PORT.getIntValue() + 1));

        // start poxy. this will block for ever
        ZMQ.proxy(in, out, null);

    }
}


