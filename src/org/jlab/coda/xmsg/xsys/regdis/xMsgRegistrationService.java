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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *     The main registrar service, that always runs in a
 *     separate thread. Contains two separate databases
 *     to store publishers and subscribers registration data.
 *     The key for the data base is xMsg topic, constructed as:
 *     <b>domain:subject:type</b>
 *     Creates REP socket server on a default port
 *     {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
 *     Following request will be serviced:
 *     <ul>
 *         <li>Register publisher</li>
 *         <li>Register subscriber</li>
 *         <li>Find publisher</li>
 *         <li>Find subscriber</li>
 *     </ul>
 *     todo: add functionality to remove publisher/subscriber
 * </p>
 *
 * @author gurjyan
 *         Created on 10/10/14
 * @version %I%
 * @since 1.0
 */

public class xMsgRegistrationService extends Thread {

    // zmq context.
    // Note. this class does not own the context.
    private ZContext context;

    // Database to store publishers
    private ConcurrentHashMap<String, xMsgRegistration>
            publishers_db =  new ConcurrentHashMap<>();

    // database to store subscribers
    private ConcurrentHashMap<String, xMsgRegistration>
            subscribers_db = new ConcurrentHashMap<>();

    // Registrar accepted requests from any host (*)
    private String host = xMsgConstants.ANY.getStringValue();

    // Default port of the registrar
    private int port = xMsgConstants.REGISTRAR_PORT.getIntValue();

    // Used as a prefix to the name of this registrar.
    // The name of the registrar is used to set the sender field
    // when it creates a request message to be sent to the requester.
    private String localhost_ip;

    /**
     * <p>
     *     Basic constructor, used primarily by xMsgFE
     * </p>
     * @param context zmq context
     * @throws xMsgException
     */
    public xMsgRegistrationService(ZContext context) throws xMsgException, SocketException {
        this.context = context;
        localhost_ip = xMsgUtil.host_to_ip("localhost");
    }

    /**
     * <p>
     *     Constructor used by xMSgNode objects.
     *     xMsgNode needs periodically report/update xMsgFe registration
     *     database with data stored in its local databases. This process
     *     makes sure we have proper duplication of the registration data
     *     for clients seeking global discovery of publishers/subscribers.
     *     It is true that discovery can be done using xMsgNode registrar
     *     service only, however by introducing xMsgFE, xMsgNodes can come
     *     and go, thus making xMsg message-space elastic.
     * </p>
     * @param feHost xMsg front-end host
     * @param context zmq context
     * @throws xMsgException
     */
    public xMsgRegistrationService(String feHost, ZContext context) throws xMsgException, SocketException {
        this.context = context;
        localhost_ip = xMsgUtil.host_to_ip("localhost");

        /**
         * Start a thread with periodic process (hard-coded 5 sec. interval) that
         * updates xMsgFE database with the data stored in the local databases:
         * @see #publishers_db
         * @see #subscribers_db
         */
        Thread t = new Thread(new xMsgRegRepT(feHost, publishers_db, subscribers_db));
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

        System.out.println(xMsgUtil.currentTime(4) +
                " Info: xMsg local registration and discovery server is started");

        //  Create registrar REP socket
        ZMQ.Socket reg_socket = context.createSocket(ZMQ.REP);
        reg_socket.bind("tcp://" + host + ":" + port);
        ZMsg request;
        while (!Thread.currentThread().isInterrupted()) {

            // Block and wait requests
            request = ZMsg.recvMsg(reg_socket);

            // Check for a null message
            if (request == null) {
                System.out.println(xMsgUtil.currentTime(4) +
                        "  Warning: received a null request...");
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
                System.out.println(xMsgUtil.currentTime(2) +
                        "  Warning: xMsg message format violation...");
                request.destroy();
                continue;
            }

            // Received message validation checks are passed
            // get message components
            ZFrame r_topic = request.pop(); // get serialized topic object
            ZFrame r_sender = request.pop(); // get serialized sender object

            // De-serialize topic and sender fields
            String s_topic = new String(r_topic.getData(), ZMQ.CHARSET);
            String s_sender = new String(r_sender.getData(), ZMQ.CHARSET);

//            System.out.println(xMsgUtil.currentTime(2) +
//                    " Received a request from " + s_sender + " to " + s_topic);

            ZFrame r_data = request.pop(); // get serialize data

            xMsgRegistration s_data = null; // registration data

            // RemoveAll request data that defines the host name where xMsg actors are registered
            String s_host = xMsgConstants.UNDEFINED.getStringValue();

            try {
                String key = xMsgConstants.UNDEFINED.getStringValue();

                if(s_topic.equals(xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue())){
                    // This is remove all request de-serialize data as string and get the host name
                    s_host = new String(r_data.getData(), ZMQ.CHARSET);

                } else {
                    // De-serialize data using protobuf to get xMsgRegistration object
                    s_data = xMsgRegistration.parseFrom(r_data.getData());

                    // construct database key (domain:subject:type)
                    key = s_data.getDomain();
                    if (!s_data.getSubject().equals(xMsgConstants.UNDEFINED.getStringValue())) {
                        key = key + ":" + s_data.getSubject();
                    }
                    if (!s_data.getType().equals(xMsgConstants.UNDEFINED.getStringValue())) {
                        key = key + ":" + s_data.getType();
                    }
                }
                // Destroy zmq frames and request message
                r_topic.destroy();
                r_sender.destroy();
                r_data.destroy();
                request.destroy();

                // Create a reply message
                ZMsg reply = new ZMsg();

                // Add received topic to the reply message (topic will
                // be the same for REQ/REP communication)
                reply.addString(s_topic);

                // Add sender = localhost_ip:xMsg_Registrar to the reply message
                reply.addString(localhost_ip+":"+xMsgConstants.REGISTRAR.getStringValue());

                // In order to add the data to the request message we need
                // actually to perform the request and define the resulting data
                // Note: requested action is passed throw the xMsg topic field.
                if (s_topic.equals(xMsgConstants.REGISTER_PUBLISHER.getStringValue())) {
                    if(s_data!=null) publishers_db.put(key, s_data);
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (s_topic.equals(xMsgConstants.REGISTER_SUBSCRIBER.getStringValue())) {
                    if(s_data!=null) subscribers_db.put(key, s_data);
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (s_topic.equals(xMsgConstants.REMOVE_PUBLISHER.getStringValue())) {
                    publishers_db.remove(key);
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (s_topic.equals(xMsgConstants.REMOVE_SUBSCRIBER.getStringValue())) {
                    subscribers_db.remove(key);
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (s_topic.equals(xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue())) {

                    // Remove publishers registration data from a specified host
                    _cleanDbByHost(s_host, publishers_db);

                    // Remove subscribers registration data from a specified host
                    _cleanDbByHost(s_host, subscribers_db);
                    reply.addString(xMsgConstants.SUCCESS.getStringValue());

                } else if (s_topic.equals(xMsgConstants.FIND_PUBLISHER.getStringValue())) {
                    assert s_data != null;
                    Set<xMsgRegistration> res = _getRegistration(s_data.getDomain(),
                            s_data.getSubject(),
                            s_data.getType(),
                            true);
                    if(!res.isEmpty()){
                        for (xMsgRegistration rd : res) {

                            // Serialize and add to the reply message
                            byte[] _sd = rd.toByteArray();
                            reply.add(_sd);
                        }
                    }

                } else if (s_topic.equals(xMsgConstants.FIND_SUBSCRIBER.getStringValue())) {
                    assert s_data != null;
                    Set<xMsgRegistration> res = _getRegistration(s_data.getDomain(),
                            s_data.getSubject(),
                            s_data.getType(),
                            false);
                    if(!res.isEmpty()){
                        for (xMsgRegistration rd : res) {

                            // Serialize and add to the reply message
                            byte[] _sd = rd.toByteArray();
                            reply.add(_sd);
                        }
                    }

                } else {
                    System.out.println(xMsgUtil.currentTime(4) +
                            " Warning: unknown registration request type...");
                    reply.destroy();
                    continue;
                }

                // Send the reply message
                reply.send(reg_socket);

                // Destroy the xmq message afterwards
                reply.destroy();

            } catch (InvalidProtocolBufferException | xMsgException e) {
                System.out.println(xMsgUtil.currentTime(4) + " " + e.getMessage());
            }
        }
        reg_socket.close();
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
     * @param type input key subject element
     * @param isPublisher defines what database to look for
     * @return set of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     */
    private Set<xMsgRegistration> _getRegistration(String domain,
                                                       String subject,
                                                       String type,
                                                       boolean isPublisher)
            throws xMsgException {

        if(domain.equals(xMsgConstants.UNDEFINED.getStringValue()) ||
                domain.equals("*")) {
            throw new xMsgException("undefined domain");
        }

        Set<xMsgRegistration> result = new HashSet<>();
        if(isPublisher) {
            for (String k : publishers_db.keySet()) {
                if((xMsgUtil.getTopicDomain(k).equals(domain)) &&
                        (xMsgUtil.getTopicSubject(k).equals(subject) ||
                                subject.equals("*") ||
                                subject.equals(xMsgConstants.UNDEFINED.getStringValue())) &&
                (xMsgUtil.getTopicType(k).equals(type) ||
                        type.equals("*") ||
                        type.equals(xMsgConstants.UNDEFINED.getStringValue()))) {
                    result.add(publishers_db.get(k));
                }
            }
        } else {
            for (String k : subscribers_db.keySet()) {

                if((xMsgUtil.getTopicDomain(k).equals(domain)) &&
                        (xMsgUtil.getTopicSubject(k).equals(subject) ||
                                subject.equals("*") ||
                                subject.equals(xMsgConstants.UNDEFINED.getStringValue())) &&
                        (xMsgUtil.getTopicType(k).equals(type) ||
                                type.equals("*") ||
                                type.equals(xMsgConstants.UNDEFINED.getStringValue()))) {
                    result.add(subscribers_db.get(k));
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
    private void _cleanDbByHost(String host, ConcurrentHashMap<String, xMsgRegistration> db) {

        // First create a list of keys that match the criteria, to
        // be used for removing the registration data for those keys
        List<String> k = new ArrayList<>();

        // Marshal through publishers data base and remove all
        // publisher registration data on a specified host
        for (Map.Entry<String, xMsgRegistration> entry : db.entrySet()) {
            if(entry.getValue().getHost().equals(host)) k.add(entry.getKey());
        }

        //Remove all identified keys
        for(String rk:k) db.remove(rk);

    }
}
