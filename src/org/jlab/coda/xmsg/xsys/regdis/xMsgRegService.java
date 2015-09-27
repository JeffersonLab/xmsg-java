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

package org.jlab.coda.xmsg.xsys.regdis;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * The main registrar service, always running in a separate thread.
 * <p>
 * Contains two separate databases to store publishers and subscribers
 * registration data. The key for the data base is the xMsg topic, constructed
 * as: {@code domain:subject:type}.
 * <p>
 * A 0MQ REP socket is created on the default port
 * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}.
 * The following requests will be serviced:
 * <ul>
 *   <li>Register publisher</li>
 *   <li>Register subscriber</li>
 *   <li>Find publisher</li>
 *   <li>Find subscriber</li>
 * </ul>
 * TODO: add functionality to remove publisher/subscriber
 *
 */

public class xMsgRegService implements Runnable {

    // zmq context.
    // Note. this class does not own the context.
    private final ZContext context;

    // Database to store publishers
    private final xMsgRegDatabase publishers = new xMsgRegDatabase();

    // database to store subscribers
    private final xMsgRegDatabase subscribers = new xMsgRegDatabase();

    // Registrar accepted requests from any host (*)
    private final String host = xMsgConstants.ANY.getStringValue();
    // Used as a prefix to the name of this registrar.
    // The name of the registrar is used to set the sender field
    // when it creates a request message to be sent to the requester.
    private final String localProxyIp;
    // Default port of the registrar
    private int proxyPort = xMsgConstants.REGISTRAR_PORT.getIntValue();

    /**
     * Constructor for the front-end registration.
     *
     * @param context the shared 0MQ context
     * @throws xMsgException if the host IP address could not be obtained.
     * @throws IOException
     */
    public xMsgRegService(ZContext context, String proxyIp, int proxyPort) throws IOException {
        this.context = context;
        this.proxyPort = proxyPort;
        localProxyIp = proxyIp;
    }


    /**
     * Constructor for the common node registration.
     * <p>
     * An xMsg node periodically reports/update the front-end registration
     * database with the data stored in its local database.
     * This process makes sure there is a proper duplication of the registration
     * data for clients seeking global discovery of publishers/subscribers.
     * <p>
     * It is true that discovery can be done using xMsgNode registrar service
     * only, however by introducing xMsgFE, xMsgNodes can come and go, thus
     * making xMsg message-space elastic.
     *
     * @param feProxyIp the host of the front-end
     * @param feProxyPort the port of the front-end proxy
     * @param proxyPort the port of the local proxy
     * @param context the shared 0MQ context
     * @throws xMsgException
     * @throws IOException
     */
    public xMsgRegService(ZContext context, String proxyIp, int proxyPort,
                          String feProxyIp, int feProxyPort)
            throws IOException  {
        this(context, proxyIp, proxyPort);
         /*
         * Start a thread with periodic process (hard-coded 5 sec. interval) that
         * updates xMsgFE database with the data stored in the local databases.
         */
        xMsgRegDriver driver = new xMsgRegDriver(context, feProxyIp, feProxyPort);
        xMsgRegUpdater updater = new xMsgRegUpdater(driver, publishers, subscribers);
        Thread t = xMsgUtil.newThread("registration-updater", updater);
        t.start();
    }

    @Override
    public void run() {
        log("Info: xMsg local registration and discovery server is started");

        ZMQ.Socket regSocket = context.createSocket(ZMQ.REP);
        regSocket.bind("tcp://" + host + ":" + proxyPort);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                ZMsg request = ZMsg.recvMsg(regSocket);
                if (request == null) {
                    continue;
                }
                ZMsg reply = processRequest(request);
                try {
                    reply.send(regSocket);
                } finally {
                    reply.destroy();
                    request.destroy();
                }
            } catch (ZMQException e) {
                if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                    break;
                }
                log(e);
            }
        }
        context.destroy();
        log("Info: shutting down xMsg local registration and discovery server");
    }


    ZMsg processRequest(ZMsg requestMsg) {
        String topic = xMsgConstants.UNDEFINED.getStringValue();
        String sender = localProxyIp + ":" + xMsgConstants.REGISTRAR.getStringValue();

        xMsgRegResponse reply;

        try {
            Set<xMsgRegistration> registration = new HashSet<>();
            xMsgRegRequest request = new xMsgRegRequest(requestMsg);
            topic = request.topic();

            if (topic.equals(xMsgConstants.REGISTER_PUBLISHER.getStringValue())) {
                publishers.register(request.data());

            } else if (topic.equals(xMsgConstants.REGISTER_SUBSCRIBER.getStringValue())) {
                subscribers.register(request.data());

            } else if (topic.equals(xMsgConstants.REMOVE_PUBLISHER.getStringValue())) {
                publishers.remove(request.data());

            } else if (topic.equals(xMsgConstants.REMOVE_SUBSCRIBER.getStringValue())) {
                subscribers.remove(request.data());

            } else if (topic.equals(xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue())) {
                publishers.remove(request.text());
                subscribers.remove(request.text());

            } else if (topic.equals(xMsgConstants.FIND_PUBLISHER.getStringValue())) {
                xMsgRegistration data = request.data();
                registration = publishers.find(data.getDomain(),
                                               data.getSubject(),
                                               data.getType());

            } else if (topic.equals(xMsgConstants.FIND_SUBSCRIBER.getStringValue())) {
                xMsgRegistration data = request.data();
                registration = subscribers.find(data.getDomain(),
                                                data.getSubject(),
                                                data.getType());
            } else {
                log("Warning: unknown registration request type...");
                reply = new xMsgRegResponse(topic, sender, "unknown registration request");
                return reply.msg();
            }

            reply = new xMsgRegResponse(topic, sender, registration);

        } catch (xMsgException | InvalidProtocolBufferException e) {
            log(e);
            reply = new xMsgRegResponse(topic, sender, e.getLocalizedMessage());
        }

        return reply.msg();
    }


    private void log(String msg) {
        System.out.println(xMsgUtil.currentTime(4) + " " + msg);
    }


    private void log(Exception e) {
        System.err.print(xMsgUtil.currentTime(4) + " ");
        e.printStackTrace();
    }
}
