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
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegistrar;
import org.zeromq.ZContext;

import java.net.SocketException;


/**
 * <p>
 *     xMsg Front-End
 *     Runs the global registrar service
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgFE {

    // zmq context
    private ZContext context = new ZContext();

    // Thread that runs the registrar service (req/rep server)
    private Thread _registrationThread;

    public static void main(String[] args) {
        final xMsgFE fe = new xMsgFE();

        // Start of the registrar service
        try {
            fe._registrationThread = new xMsgRegistrar(fe.context);
        } catch (xMsgException | SocketException e) {
            e.printStackTrace();
        }
        fe._registrationThread.start();
        System.out.println(xMsgUtil.currentTime(4) +
                " Info: xMsg FE registration and discovery server is started");

        // Shutdown hook, thread that will run jvm before exiting
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                super.run();
                System.out.println(xMsgUtil.currentTime(4) +
                        " Info: Shutting down xMsg FE registration and discovery server");
                fe._registrationThread.interrupt();
                fe.context.close();
                System.out.println("bye...");
            }
        });

        xMsgUtil.sleep_fe(3000);

    }
}
