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

package org.jlab.coda.xmsg.xsys.regdis;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.util.HashSet;
import java.util.Set;

/**
 * The main registrar service or registrar (names are used interchangeably).
 * Note that the object of this class always running in a separate thread.
 * This class is used by the xMsgRegistrar executable.
 * <p>
 * Creates and maintains two separate databases to store publishers and subscribers
 * <p>
 * The following requests will be serviced:
 * <ul>
 *   <li>Register publisher</li>
 *   <li>Register subscriber</li>
 *   <li>Find publisher</li>
 *   <li>Find subscriber</li>
 * </ul>
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgRegService implements Runnable {

    // 0MQ context.
    private final ZContext shadowContext;

    // Database to store publishers
    private final xMsgRegDatabase publishers = new xMsgRegDatabase();

    // database to store subscribers
    private final xMsgRegDatabase subscribers = new xMsgRegDatabase();

    // Address of the registrar
    private final xMsgRegAddress regAddress;

    // Address of the registrar
    private final Socket regSocket;


    /**
     * Creates an xMsg registrar object.
     *
     * @param context the context to run the registrar service
     * @param address the address of the registrar service
     * @see ZContext#shadow
     */
    public xMsgRegService(ZContext context, xMsgRegAddress address) {
        shadowContext = ZContext.shadow(context);
        regAddress = address;
        try {
            regSocket = shadowContext.createSocket(ZMQ.REP);
            regSocket.bind("tcp://" + regAddress.host() + ":" + regAddress.port());
        } catch (ZMQException e) {
            shadowContext.destroy();
            throw e;
        }
    }

    /**
     * Returns the address of the registrar.
     */
    public xMsgRegAddress address() {
        return regAddress;
    }

    @Override
    public void run() {
        printStartup();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ZMsg request = ZMsg.recvMsg(regSocket);
                if (request == null) {
                    continue;
                }
                ZMsg reply = processRequest(request);
                reply.send(regSocket);
            } catch (ZMQException e) {
                if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                    break;
                }
                log(e);
            }
        }
        shadowContext.destroy();
        log("xMsg-Info: shutting down xMsg registration and discovery server");
    }

    /**
     * Registration request processing routine that runs in this thread.
     *
     * @param requestMsg serialized 0MQ message of the wire
     * @return serialized response: 0MQ message ready to go over the wire
     */
    ZMsg processRequest(ZMsg requestMsg) {

        // Preparing fields to furnish the response back.
        // Note these fields do not play any critical role what so ever, due to
        // the fact that registration is done using client-server type communication,
        // and are always synchronous.
        String topic = xMsgConstants.UNDEFINED;
        String sender = regAddress.host() + ":" + xMsgConstants.REGISTRAR;

        // response message
        xMsgRegResponse reply;

        try {
            // prepare the set to store registration info going back to the requester
            Set<xMsgRegistration> registration = new HashSet<>();

            // create a xMsgRegRequest object from the serialized 0MQ message
            xMsgRegRequest request = new xMsgRegRequest(requestMsg);

            // retrieve the topic
            topic = request.topic();

            if (topic.equals(xMsgConstants.REGISTER_PUBLISHER)) {
                publishers.register(request.data());

            } else if (topic.equals(xMsgConstants.REGISTER_SUBSCRIBER)) {
                subscribers.register(request.data());

            } else if (topic.equals(xMsgConstants.REMOVE_PUBLISHER)) {
                publishers.remove(request.data());

            } else if (topic.equals(xMsgConstants.REMOVE_SUBSCRIBER)) {
                subscribers.remove(request.data());

            } else if (topic.equals(xMsgConstants.REMOVE_ALL_REGISTRATION)) {
                publishers.remove(request.text());
                subscribers.remove(request.text());

            } else if (topic.equals(xMsgConstants.FIND_PUBLISHER)) {
                xMsgRegistration data = request.data();
                registration = publishers.find(data.getDomain(),
                                               data.getSubject(),
                                               data.getType());

            } else if (topic.equals(xMsgConstants.FIND_SUBSCRIBER)) {
                xMsgRegistration data = request.data();
                registration = subscribers.find(data.getDomain(),
                                                data.getSubject(),
                                                data.getType());

            }  else {
                log("xMsg-Warning: unknown registration request type");
                reply = new xMsgRegResponse(topic, sender, "unknown registration request");
                return reply.msg();
            }

            reply = new xMsgRegResponse(topic, sender, registration);

        } catch (xMsgException | InvalidProtocolBufferException e) {
            log(e);
            reply = new xMsgRegResponse(topic, sender, e.getLocalizedMessage());
        } finally {
            requestMsg.destroy();
        }

        return reply.msg();
    }

    private void printStartup() {
        String regAddr = "tcp://" + regAddress.host() + ":" + regAddress.port();
        String logMsg = " xMsg-Info: registration and discovery server is started at address = ";
        log(xMsgUtil.currentTime(4) + logMsg + regAddr);
    }

    /**
     * Prints on a stdio with an appropriate time stamp.
     */
    private void log(String msg) {
        System.out.println(xMsgUtil.currentTime(4) + " " + msg);
    }

    /**
     * Prints exception stacktrace with an appropriate time stamp.
     */
    private void log(Exception e) {
        System.err.print(xMsgUtil.currentTime(4) +
                " message = " + e.getMessage() + " cause = " + e.getCause() + " ");
        e.printStackTrace();
    }
}
