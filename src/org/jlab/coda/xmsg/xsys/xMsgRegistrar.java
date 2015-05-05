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
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDiscDriver;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegistrationService;
import org.zeromq.ZContext;

import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * xMsgRegistrar.
 * Note that no arg constructed object can play master registrar role.
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgRegistrar extends xMsgRegDiscDriver {

    // shared memory of the node (in the language of CLARA it would be DPE)
    public static final ConcurrentHashMap<String, xMsgMessage> _shared_memory = new ConcurrentHashMap<>();


    /**
     * <p>
     * Starts a local registrar service.
     * Note: this version assumes that xMsgNode and xMsgFE registrar services
     * use default registrar port:
     * {@link xMsgConstants#REGISTRAR_PORT}
     * </p>
     *
     * @throws xMsgException
     */
    public xMsgRegistrar() throws xMsgException, SocketException {

        super("localhost");

        // Get local IP addresses in case of multiple network cards.
        // This list is available through xMsgUtil.LOCAL_HOST_IPS
        xMsgUtil.updateLocalHostIps();

        // Zmq context
        final ZContext _context = new ZContext();

        // start registrar service
        new xMsgRegistrationService(_context).run();
    }


    /**
     * <p>
     * Starts a local registrar service.
     * Constructor of the {@link org.jlab.coda.xmsg.xsys.regdis.xMsgRegistrationService} class
     * will start a thread that will periodically report local registration
     * database to xMsgFE ({@link xMsgFE}registrar service.
     * This ways registration data is distributed/duplicated between xMsgNode
     * and xMsgFE registrar services.
     * That is the reason we need to pass xMsg front-end host name.
     * Note: this version assumes that xMsgNode and xMsgFE registrar services
     * use default registrar port:
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * </p>
     *
     * @param feHost xMsg front-end host. Host is passed through command line -h option,
     *               or through the environmental variable: XMSG_FE_HOST
     * @throws xMsgException
     */
    public xMsgRegistrar(final String feHost) throws xMsgException, SocketException {

        super(feHost);

        // Get local IP addresses in case of multiple network cards.
        // This list is available through xMsgUtil.LOCAL_HOST_IPS
        xMsgUtil.updateLocalHostIps();

        // Zmq context
        final ZContext _context = new ZContext();

        // Start local registrar service.
        // In this case this specific constructor starts a thread
        // that periodically updates front-end registrar database with
        // the data from the local databases

        new xMsgRegistrationService(feHost, _context).run();
    }


    public static void main(String[] args) {

        if (args.length == 2) {
            if (args[0].equals("-fe_host")) {
                try {
                    new xMsgRegistrar(args[1]);
                } catch (xMsgException e) {
                    System.out.println(e.getMessage());
                    System.out.println("exiting...");
                } catch (SocketException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("wrong option. Accepts -fe_host option only.");
            }
        } else if (args.length == 0) {
            try {
                new xMsgRegistrar();
            } catch (xMsgException e) {
                System.out.println(e.getMessage());
                System.out.println("exiting...");
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }
    }
}


