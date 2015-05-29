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
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.net.SocketException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 * @author gurjyan
 * @since 1.0
 */

public class xMsgRegistrationService extends Thread {

    // zmq context.
    // Note. this class does not own the context.
    private final ZContext context;

    // Database to store publishers
    private final ConcurrentMap<String, Set<xMsgRegistration>>
            publishers =  new ConcurrentHashMap<>();

    // database to store subscribers
    private final ConcurrentMap<String, Set<xMsgRegistration>>
            subscribers = new ConcurrentHashMap<>();

    // Registrar accepted requests from any host (*)
    private final String host = xMsgConstants.ANY.getStringValue();

    // Default port of the registrar
    private final int port = xMsgConstants.REGISTRAR_PORT.getIntValue();

    // Used as a prefix to the name of this registrar.
    // The name of the registrar is used to set the sender field
    // when it creates a request message to be sent to the requester.
    private final String localhost;

    /**
     * Constructor for the front-end registration.
     *
     * @param context the shared 0MQ context
     * @throws SocketException if an I/O error occurs.
     * @throws xMsgException if the host IP address could not be obtained.
     */
    public xMsgRegistrationService(ZContext context) throws SocketException, xMsgException {
        this.context = context;
        localhost = xMsgUtil.toHostAddress("localhost");
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
     * @param feHost the host of the front-end
     * @param context the shared 0MQ context
     * @throws xMsgException
     * @throws SocketException
     * @throws IOException
     */
    public xMsgRegistrationService(String feHost, ZContext context)
            throws SocketException, xMsgException {
        this.context = context;
        localhost = xMsgUtil.toHostAddress("localhost");

        /*
         * Start a thread with periodic process (hard-coded 5 sec. interval) that
         * updates xMsgFE database with the data stored in the local databases.
         */
        Thread t = new Thread(new xMsgRegRepT(feHost, publishers, subscribers));
        t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();
            }
        });
        t.start();
    }

    @Override
    public void run() {
        super.run();

        System.out.println(xMsgUtil.currentTime(4)
                + " Info: xMsg local registration and discovery server is started");

        //  Create registrar REP socket
        ZMQ.Socket regSocket = context.createSocket(ZMQ.REP);
        regSocket.bind("tcp://" + host + ":" + port);
        ZMsg request;
        while (!Thread.currentThread().isInterrupted()) {

            // Block and wait requests
            request = ZMsg.recvMsg(regSocket);

            // Check for a null message
            if (request == null) {
                System.out.println(xMsgUtil.currentTime(4)
                        + "  Warning: received a null request...");
                continue;
            }

            /**
             * Check for xMsg message format violation.
             * Note: xMsg has 3 part construct:
             * <ul>
             *     <li>topic</li>
             *     <li>sender</li>
             *     <li>data</li>
             * </ul>
             */
            if (request.size() != 3) {
                System.out.println(xMsgUtil.currentTime(2)
                        + "  Warning: xMsg message format violation...");
                request.destroy();
                continue;
            }

            // Received message validation checks are passed
            // get message components
            ZFrame reqTopicFrame = request.pop(); // get serialized topic object
            ZFrame reqSenderFrame = request.pop(); // get serialized sender object

            // De-serialize topic and sender fields
            String reqTopic = new String(reqTopicFrame.getData(), ZMQ.CHARSET);
//            String repSender = new String(repSenderFrame.getData(), ZMQ.CHARSET);
//            System.out.println(xMsgUtil.currentTime(2)
//                    + " Received a request from " + repSender + " to " + repTopic);

            ZFrame reqDataFrame = request.pop(); // get serialize data

            xMsgRegistration reqData = null; // registration data

            // RemoveAll request data that defines the host name where xMsg actors are registered
            String reqHost = xMsgConstants.UNDEFINED.getStringValue();

            try {
                String key = xMsgConstants.UNDEFINED.getStringValue();

                if (reqTopic.equals(xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue())) {
                    // This is remove all request de-serialize data as string and get the host name
                    reqHost = new String(reqDataFrame.getData(), ZMQ.CHARSET);

                } else {
                    // De-serialize data using protobuf to get xMsgRegistration object
                    reqData = xMsgRegistration.parseFrom(reqDataFrame.getData());

                    // construct database key (domain:subject:type)
                    key = reqData.getDomain();
                    if (!reqData.getSubject().equals(xMsgConstants.UNDEFINED.getStringValue())) {
                        key = key + ":" + reqData.getSubject();
                    }
                    if (!reqData.getType().equals(xMsgConstants.UNDEFINED.getStringValue())) {
                        key = key + ":" + reqData.getType();
                    }
                }
                // Destroy zmq frames and request message
                reqTopicFrame.destroy();
                reqSenderFrame.destroy();
                reqDataFrame.destroy();
                request.destroy();

                // Create a reply message
                ZMsg reply = new ZMsg();

                // Add received topic to the reply message (topic will
                // be the same for REQ/REP communication)
                reply.addString(reqTopic);

                // Add sender = localhost_ip:xMsg_Registrar to the reply message
                reply.addString(localhost + ":" + xMsgConstants.REGISTRAR.getStringValue());

                // In order to add the data to the request message we need
                // actually to perform the request and define the resulting data
                // Note: requested action is passed throw the xMsg topic field.
                if (reqTopic.equals(xMsgConstants.REGISTER_PUBLISHER.getStringValue())) {
                    if (reqData != null) {
                        if (publishers.containsKey(key)) {
                            publishers.get(key).add(reqData);
                        } else {
                            Set<xMsgRegistration> tmset = new HashSet<>();
                            tmset.add(reqData);
                            publishers.put(key, tmset);
                        }
                    }
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (reqTopic.equals(xMsgConstants.REGISTER_SUBSCRIBER.getStringValue())) {
                    if (reqData != null) {
                        if (subscribers.containsKey(key)) {
                            subscribers.get(key).add(reqData);
                        } else {
                            Set<xMsgRegistration> tmset = new HashSet<>();
                            tmset.add(reqData);
                            subscribers.put(key, tmset);
                        }
                    }
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (reqTopic.equals(xMsgConstants.REMOVE_PUBLISHER.getStringValue())) {
                    if (publishers.containsKey(key)) {
                        publishers.get(key).remove(reqData);
                        if (publishers.get(key).isEmpty()) {
                            publishers.remove(key);
                        }
                    }
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (reqTopic.equals(xMsgConstants.REMOVE_SUBSCRIBER.getStringValue())) {
                    if (subscribers.containsKey(key)) {
                        subscribers.get(key).remove(reqData);
                        if (subscribers.get(key).isEmpty()) {
                            subscribers.remove(key);
                        }
                    }
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (reqTopic.equals(xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue())) {

                    // Remove publishers registration data from a specified host
                    cleanDbByHost(reqHost, publishers);

                    // Remove subscribers registration data from a specified host
                    cleanDbByHost(reqHost, subscribers);
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (reqTopic.equals(xMsgConstants.FIND_PUBLISHER.getStringValue())) {
                    assert reqData != null;
                    Set<xMsgRegistration> res = getRegistration(reqData.getDomain(),
                                                                reqData.getSubject(),
                                                                reqData.getType(),
                                                                true);
                    if (!res.isEmpty()) {
                        for (xMsgRegistration rd : res) {
                            // Serialize and add to the reply message
                            reply.add(rd.toByteArray());
                        }
                    }

                } else if (reqTopic.equals(xMsgConstants.FIND_SUBSCRIBER.getStringValue())) {
                    assert reqData != null;
                    Set<xMsgRegistration> res = getRegistration(reqData.getDomain(),
                                                                reqData.getSubject(),
                                                                reqData.getType(),
                                                                false);
                    if (!res.isEmpty()) {
                        for (xMsgRegistration rd : res) {
                            // Serialize and add to the reply message
                            reply.add(rd.toByteArray());
                        }
                    }

                } else {
                    System.out.println(xMsgUtil.currentTime(4) +
                            " Warning: unknown registration request type...");
                    reply.destroy();
                    continue;
                }

                // Send the reply message
                reply.send(regSocket);

                // Destroy the xmq message afterwards
                reply.destroy();

            } catch (InvalidProtocolBufferException | xMsgException e) {
                System.out.println(xMsgUtil.currentTime(4) + " " + e.getMessage());
            }
        }
        regSocket.close();
    }

    /**
     * <p>
     *     This method finds registration data in the database based on
     *     a match of the xMsg registration database-key components.
     *     We assume that domain element of the key  is always defined,
     *     but subject and types can be set to be undefined. So, what we
     *     do here is to create a list containing only defined components
     *     of the input key, and retrieve registration objects of those
     *     database keys containing defined elements of the input key.
     * </p>
     * @param domain input key domain element
     * @param subject input key subject element
     * @param type input key type element
     * @param isPublisher defines what database to look for
     * @return set of {@link xMsgRegistration} objects
     */
    private Set<xMsgRegistration> getRegistration(String domain,
                                                  String subject,
                                                  String type,
                                                  boolean isPublisher)
            throws xMsgException {

        if (domain.equals(xMsgConstants.UNDEFINED.getStringValue()) ||
                domain.equals("*")) {
            throw new xMsgException("undefined domain");
        }

        Set<xMsgRegistration> result = new HashSet<>();
        if (isPublisher) {
            for (String k : publishers.keySet()) {
                if ((xMsgUtil.getTopicDomain(k).equals(domain)) &&
                        (xMsgUtil.getTopicSubject(k).equals(subject) ||
                                subject.equals("*") ||
                                subject.equals(xMsgConstants.UNDEFINED.getStringValue())) &&
                        (xMsgUtil.getTopicType(k).equals(type) ||
                                type.equals("*") ||
                                type.equals(xMsgConstants.UNDEFINED.getStringValue()))) {
                    result.addAll(publishers.get(k));
                }
            }
        } else {
            for (String k : subscribers.keySet()) {
                if ((xMsgUtil.getTopicDomain(k).equals(domain)) &&
                        (xMsgUtil.getTopicSubject(k).equals(subject) ||
                                subject.equals("*") ||
                                subject.equals(xMsgConstants.UNDEFINED.getStringValue())) &&
                        (xMsgUtil.getTopicType(k).equals(type) ||
                                type.equals("*") ||
                                type.equals(xMsgConstants.UNDEFINED.getStringValue()))) {
                    result.addAll(subscribers.get(k));
                }
            }
        }
        return result;
    }

    /**
     * Method that removes all values of the registration database that
     * have a specified host set, i.e.  removes registration information
     * of all xMsg actors that are running on a specified host.
     * @param host host name
     * @param db reference to the registration database
     */
    private void cleanDbByHost(String host,
                               ConcurrentMap<String, Set<xMsgRegistration>> db) {

        Set<xMsgRegistration> tmps = new HashSet<>();

        // Marshal through publishers data base and remove all
        // publisher registration data on a specified host
        for (String key: db.keySet()) {
            for (xMsgRegistration r:db.get(key)) {
                if (r.getHost().equals(host)) {
                    tmps.add(r);
                }
            }
            db.get(key).removeAll(tmps);
            tmps.clear();
        }
    }
}
