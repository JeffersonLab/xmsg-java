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

package org.jlab.coda.xmsg.core;

import com.google.protobuf.InvalidProtocolBufferException;

import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgDiscoverException;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDiscDriver;
import org.jlab.coda.xmsg.xsys.xMsgRegistrar;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.jlab.coda.xmsg.xsys.regdis.xMsgRegDiscDriver.__zmqSocket;

/**
 * <p>
 *     xMsg base class that provides methods for
 *     organizing pub/sub communications. This class
 *     provides a local database of xMsgCommunication
 *     for publishing and/or subscribing messages without
 *     requesting registration information from the local
 *     registrar services.
 *     This class also provides a thread pool for servicing
 *     received messages (as a result of a subscription) in
 *     separate threads.
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */

public class xMsg {

    // zmq context object
    private ZContext _context;

    // Private db of stored connections
    private HashMap<String, xMsgConnection> _connections = new HashMap<>();

    // Fixed size thread pool
    private ExecutorService threadPool = null;
    // default thread pool size
    private int pool_size = 2;

    private xMsgRegDiscDriver driver;


    /**
     * <p>
     *     Constructor, requires the name of the FrontEnd host that is used
     *     to create a connection to the registrar service running within the
     *     xMsgFE. Creates the zmq context object and thread pool for servicing
     *     received messages in a separate threads.
     * </p>
     * @param feHost host name of the FE
     * @throws xMsgException
     */
    public xMsg(String feHost) throws xMsgException, SocketException {
        /**
         * <p>
         *     Calls xMsgRegDiscDriver class constructor that creates sockets to
         *     2 registrar request/reply servers running in the local
         *     xMsgNode and xMsgFE.
         * </p>
         */
        driver = new xMsgRegDiscDriver(feHost);
        _context = driver.getContext();

        threadPool = newFixedThreadPool(this.pool_size);
    }

    /**
     * <p>
     *     Constructor, requires the name of the FrontEnd host that is used
     *     to create a connection to the registrar service running within the
     *     xMsgFE. Creates the zmq context object and thread pool for servicing
     *     received messages in a separate threads.
     * </p>
     * @param feHost host name of the FE
     * @param pool_size thread pool size for servicing subscription callbacks
     * @throws xMsgException
     */
    public xMsg(String feHost, int pool_size) throws xMsgException, SocketException {
        /**
         * <p>
         *     Calls xMsgRegDiscDriver class constructor that creates sockets to
         *     2 registrar request/reply servers running in the local
         *     xMsgNode and xMsgFE.
         * </p>
         */
        driver = new xMsgRegDiscDriver(feHost);
        _context = driver.getContext();

        // create fixed size thread pool
        this.pool_size = pool_size;
        threadPool = newFixedThreadPool(this.pool_size);
    }

    /**
     * <p>
     *     Connects to the xMsgNode by creating two sockets for publishing and
     *     subscribing/receiving messages. It returns and the same time stores
     *     in the local connection database created
     *     {@link org.jlab.coda.xmsg.net.xMsgConnection} object.
     *     This constructor makes connection to the local xMsg node, i.e.
     *     xMsgNode should be running on the localhost.
     * </p>
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     * @throws xMsgException
     */
    public xMsgConnection connect() throws xMsgException, SocketException {
        return connect(new xMsgAddress("localhost"));
    }

    /**
     * <p>
     *     Connects to the xMsgNode by creating two sockets for publishing and
     *     subscribing/receiving messages. It returns and the same time stores
     *     in the local connection database created
     *     {@link org.jlab.coda.xmsg.net.xMsgConnection} object.
     * </p>
     * @param host  xMsg node host name
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     * @throws xMsgException
     */
    public xMsgConnection connect(String host) throws xMsgException, SocketException {
        return connect(new xMsgAddress(host));
    }

    /**
     * <p>
     *     Connects to the xMsgNode by creating two sockets for publishing and
     *     subscribing/receiving messages. It returns and the same time stores
     *     in the local connection database created
     *     {@link org.jlab.coda.xmsg.net.xMsgConnection} object.
     * </p>
     * @param host  xMsg node host name
     * @param port  xMsg node port number
     * @return {@link org.jlab.coda.xmsg.net.xMsgConnection} object
     * @throws xMsgException\
     */
    public xMsgConnection connect(String host, int port) throws xMsgException, SocketException {
        return connect(new xMsgAddress(host, port));
    }

    /**
     * <p>
     *     Connects to the xMsgNode by creating two sockets for publishing and
     *     subscribing/receiving messages. It returns and the same time stores
     *     in the local connection database created
     *     {@link org.jlab.coda.xmsg.net.xMsgConnection} object.
     * </p>
     * @param address {@link xMsgAddress} object
     * @return {@link xMsgConnection} object
     * @throws xMsgException
     */
    public xMsgConnection connect(xMsgAddress address) throws xMsgException {

        /**
         * First check to see if we have already
         * established connection to this address
         */
        if(_connections.containsKey(address.getKey())) return _connections.get(address.getKey());
        else {
            /**
             *  Otherwise create sockets to the
             *  requested address, and store the created
             *  connection object for the future use.
             *  Return the reference to the connection object
             *
             */
            xMsgConnection feCon = new xMsgConnection();
            feCon.setAddress(address);
            feCon.setPubSock(__zmqSocket(_context, ZMQ.PUB, address.getHost(),
                    address.getPort(), xMsgConstants.CONNECT.getIntValue()));

            feCon.setSubSock(__zmqSocket(_context, ZMQ.SUB, address.getHost(),
                    address.getPort() + 1, xMsgConstants.CONNECT.getIntValue()));

            _connections.put(address.getKey(),feCon);
            return feCon;
        }
    }

    /**
     * <p>
     *     Connects to the xMsgNode by creating two sockets for publishing and
     *     subscribing/receiving messages.
     *     This is method will be used in case one need to have multiple
     *     threads using the same zmq socket. Note: do not try to
     *     use the same socket from multiple threads. Each thread should
     *     create it's own zmq socket and a context.
     * </p>
     * @param address {@link xMsgAddress} object
     * @return {@link xMsgConnection} object
     * @throws xMsgException
     */
    public xMsgConnection getNewConnection(xMsgAddress address) throws xMsgException {

        ZContext context = new ZContext();

        xMsgConnection feCon = new xMsgConnection();
        feCon.setAddress(address);
        feCon.setPubSock(__zmqSocket(context, ZMQ.PUB, address.getHost(),
                address.getPort(), xMsgConstants.CONNECT.getIntValue()));

        feCon.setSubSock(__zmqSocket(context, ZMQ.SUB, address.getHost(),
                address.getPort() + 1, xMsgConstants.CONNECT.getIntValue()));
        return feCon;
    }

    /**
     * <p>
     *     If you are periodically publishing data, use this method to
     *     register yourself as a publisher with the local registrar.
     *     This is necessary for future subscribers to discover and
     *     listen to your messages.
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @throws xMsgRegistrationException
     */
    public void registerPublisher(String name,
                                  String host,
                                  int port,
                                  String domain,
                                  String subject,
                                  String type)
            throws xMsgRegistrationException {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        xMsgRegistration r_data = regb.build();
        driver.register_local(name, r_data, true);
    }

    /**
     * <p>
     *     If you are periodically publishing data, use this method to
     *     register yourself as a publisher with the local registrar.
     *     This is necessary for future subscribers to discover and
     *     listen to your messages.
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @throws xMsgRegistrationException
     */
    public void registerPublisher(String name,
                                  String domain,
                                  String subject,
                                  String type)
            throws xMsgRegistrationException {
        registerPublisher(name, "localhost", xMsgConstants.DEFAULT_PORT.getIntValue(),
                domain, subject, type);
    }

    /**
     * <p>
     *     If you are a subscriber and want to listen messages on a specific
     *     topic from a future publishers, you should register yourself as
     *     a subscriber with the local registrar.
     *     Future publishers might express an interest to publish data to a
     *     a required topic of interest or might publish data only if there
     *     are active listeners/subscribers to their published topic.
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @param description description
     * @throws xMsgRegistrationException
     */
    public void registerSubscriber(String name,
                                   String host,
                                   int port,
                                   String domain,
                                   String subject,
                                   String type,
                                   String description)
            throws xMsgRegistrationException {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        regb.setDescription(description);
        xMsgRegistration r_data = regb.build();
        driver.register_local(name, r_data, false);
    }

    /**
     * <p>
     *     If you are a subscriber and want to listen messages on a specific
     *     topic from a future publishers, you should register yourself as
     *     a subscriber with the local registrar.
     *     Future publishers might express an interest to publish data to a
     *     a required topic of interest or might publish data only if there
     *     are active listeners/subscribers to their published topic.
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @param description description
     * @throws xMsgRegistrationException
     */
    public void registerSubscriber(String name,
                                   String domain,
                                   String subject,
                                   String type,
                                   String description)
            throws xMsgRegistrationException {
        registerSubscriber(name,
                "localhost",
                xMsgConstants.DEFAULT_PORT.getIntValue(),
                domain, subject, type, description);
    }

    /**
     * <p>
     *   Removes publisher registration both from the local and then from the
     *   global registration databases
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @throws xMsgRegistrationException
     */
    public void removePublisherRegistration (String name,
                                             String domain,
                                             String subject,
                                             String type)
            throws xMsgRegistrationException {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        xMsgRegistration r_data = regb.build();
        driver.removeRegistration_local(name, r_data, true);
        driver.removeRegistration_fe(name, r_data, true);
    }

    /**
     * <p>
     *   Removes subscribers registration both from the local and then from the
     *   global registration databases
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @throws xMsgRegistrationException
     */
    public void removeSubscriberRegistration (String name,
                                              String domain,
                                              String subject,
                                              String type)
            throws xMsgRegistrationException {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        xMsgRegistration r_data = regb.build();
        driver.removeRegistration_local(name, r_data, false);
        driver.removeRegistration_fe(name, r_data, false);
    }

    /**
     * <p>
     *     Finds all global (deployed anywhere in the xMsg cloud, i.e. nodes)
     *     publishers, publishing  to a specified topic
     *     Note: xMsg defines a topic as domain:subject:type
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistration> findPublishers(String name,
                                                 String host,
                                                 int port,
                                                 String domain,
                                                 String subject,
                                                 String type)
            throws xMsgDiscoverException {

        if(domain.equals("*")) throw new xMsgDiscoverException("malformed xMsg topic");
        if(subject.equals("*")) subject = xMsgConstants.UNDEFINED.getStringValue();
        if(type.equals("*")) type = xMsgConstants.UNDEFINED.getStringValue();

        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        xMsgRegistration r_data = regb.build();
        return driver.findGlobal(name, r_data, true);
    }

    /**
     * <p>
     *     Finds all global (deployed anywhere in the xMsg cloud, i.e. nodes)
     *     publishers, publishing  to a specified topic
     *     Note: xMsg defines a topic as domain:subject:type
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistration> findPublishers(String name,
                                                 String domain,
                                                 String subject,
                                                 String type)
            throws xMsgDiscoverException {
        return findPublishers(name, "localhost", xMsgConstants.REGISTRAR_PORT.getIntValue(),
                domain, subject, type);
    }

    /**
     * <p>
     *     Finds all global (deployed anywhere in the xMsg cloud, i.e. nodes)
     *     subscribers, subscribing  to a specified topic
     *     Note: xMsg defines a topic as domain:subject:type
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistration> findSubscribers(String name,
                                                  String host,
                                                  int port,
                                                  String domain,
                                                  String subject,
                                                  String type)
            throws xMsgDiscoverException {

        if(domain.equals("*")) throw new xMsgDiscoverException("malformed xMsg topic");
        if(subject.equals("*")) subject = xMsgConstants.UNDEFINED.getStringValue();
        if(type.equals("*")) type = xMsgConstants.UNDEFINED.getStringValue();

        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        xMsgRegistration r_data = regb.build();
        return driver.findGlobal(name, r_data, false);
    }

    /**
     * <p>
     *     Finds all local subscribers, subscribing  to a specified topic
     *     Note: xMsg defines a topic as domain:subject:type
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistration> findSubscribers(String name,
                                                  String domain,
                                                  String subject,
                                                  String type)
            throws xMsgDiscoverException {
        return findSubscribers(name, "localhost", xMsgConstants.REGISTRAR_PORT.getIntValue(),
                domain, subject, type);
    }

    /**
     * <p>
     *     Checks the global registration (running on the xMsg FE)
     *     to see if there a publisher publishing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one publisher is found
     * @throws xMsgDiscoverException
     */
    public boolean isTherePublisher(String name,
                                    String host,
                                    int port,
                                    String domain,
                                    String subject,
                                    String type) throws xMsgDiscoverException {
        return findPublishers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Checks the global registration (running on the xMsg FE)
     *     to see if there a publisher publishing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one publisher is found
     * @throws xMsgDiscoverException
     */
    public boolean isTherePublisher(String name,
                                    String domain,
                                    String subject,
                                    String type) throws xMsgDiscoverException {
        return findPublishers(name, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Checks the global registration (running on the xMsg FE)
     *     to see if there a subscriber subscribing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgDiscoverException
     */
    public boolean isThereSubscriber(String name,
                                     String host,
                                     int port,
                                     String domain,
                                     String subject,
                                     String type) throws xMsgDiscoverException {
        return findSubscribers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Checks the global registration (running on the xMsg FE)
     *     to see if there a subscriber subscribing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgDiscoverException
     */
    public boolean isThereSubscriber(String name,
                                     String domain,
                                     String subject,
                                     String type) throws xMsgDiscoverException {
        return findSubscribers(name, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Checks the local registration (running on the xMsg FE)
     *     to see if there a publisher publishing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one publisher is found
     * @throws xMsgDiscoverException
     */
    public boolean isThereLocalPublisher(String name,
                                         String host,
                                         int port,
                                         String domain,
                                         String subject,
                                         String type) throws xMsgDiscoverException {
        return findPublishers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Checks the local registration (running on the xMsg FE)
     *     to see if there a publisher publishing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one publisher is found
     * @throws xMsgDiscoverException
     */
    public boolean isThereLocalPublisher(String name,
                                         String domain,
                                         String subject,
                                         String type) throws xMsgDiscoverException {
        return findPublishers(name, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Checks the local registration (running on the xMsg FE)
     *     to see if there a subscriber subscribing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgDiscoverException
     */
    public boolean isThereLocalSubscriber(String name,
                                          String host,
                                          int port,
                                          String domain,
                                          String subject,
                                          String type) throws xMsgDiscoverException {
        return findSubscribers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Checks the local registration (running on the xMsg FE)
     *     to see if there a subscriber subscribing to a specified
     *     topic.
     * </p>
     *
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgDiscoverException
     */
    public boolean isThereLocalSubscriber(String name,
                                          String domain,
                                          String subject,
                                          String type) throws xMsgDiscoverException {
        return findSubscribers(name, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Publishes data to a topic defined in a specified xMsgMessage
     * </p>
     * @param connection xMsgConnection object
     * @param x_msg {@link xMsgMessage} object
     * @throws xMsgException
     */
    public void publish(xMsgConnection connection,
                        xMsgMessage x_msg)
            throws xMsgException, IOException {

        // check connection
        Socket con = connection.getPubSock();
        if (con == null) {
            System.out.println("Error: null connection object");
            throw new xMsgException("Error: null connection object");
        }

        // check message
        if (x_msg == null) {
            System.out.println("Error: null message");
            throw new xMsgException("Error: null message");
        }

        // byte array for holding the serialized metadata and data object
        byte[] metadata_s;
        byte[] data_s;

        // send topic, sender, followed by the metadata and data
        ZMsg z_msg = new ZMsg();

        // adding topic frame = 1
        z_msg.addString(x_msg.getTopic());

        xMsgMeta.Builder metadata = x_msg.getMetaData();
        if (metadata.getDataType().equals(xMsgMeta.DataType.UNDEFINED)) {
            System.out.println("Error: undefined data type");
            throw new xMsgException("Error: undefined data type");
        }

        // publishing over process or network boundaries
        if (metadata.getIsDataSerialized()) {

            // adding message location frame = 2
            z_msg.add("envelope");

            // adding metadata frame = 3
            xMsgMeta md = metadata.build();
            metadata_s = md.toByteArray();
            z_msg.add(metadata_s);

            // We serialize here only X_Object and J_object type data.
            // The rest we assume under user responsibility
            if (metadata.getDataType().equals(xMsgMeta.DataType.X_Object)) {
                // Get the data from the xMsgMessage
                xMsgData.Builder data = (xMsgData.Builder) x_msg.getData();
                // xMsgData serialization
                xMsgData dd = data.build();
                data_s = dd.toByteArray();

            } else if (metadata.getDataType().equals(xMsgMeta.DataType.J_Object)) {
                Object data = x_msg.getData();
                // Java object serialization
                data_s = xMsgUtil.O2B(data);

            } else {
                // NO serialization
                data_s = (byte[]) x_msg.getData();
            }

            // adding data frame = 4
            z_msg.add(data_s);

        } else {
            // Publishing within the same process (for e.g. DPE). In this case
            // xMsgMessage including the data and the metadata is in the shared memory.
            // Check to see if the location of the object in the shared memory is defined

            // Add the xMsgMessage (including metadata and data) to the shared memory
            // Here we define a unique key for this message in the shared memory, using
            // senders unique name and the communication ID set by the user.
            String key = metadata.getSender() + "_" + metadata.getCommunicationId();
            xMsgRegistrar._shared_memory.put(key, x_msg);

            // add message location frame = 2
            z_msg.add(key);
        }

        if (!z_msg.send(con)) {
            System.out.println("Error: publishing the message");
            throw new xMsgException("Error: publishing the message");
        }
        z_msg.destroy();

    }

    public xMsgMessage sync_publish(xMsgConnection connection,
                                    xMsgMessage x_msg,
                                    int timeOut)
            throws xMsgException,
            TimeoutException,
            IOException {

        // address/topic where the subscriber should send the result
        String returnAddress = "return:" + (int) (Math.random() * 100.0);

        // set the return address as replyTo in the xMsgMessage
        x_msg.getMetaData().setReplyTo(returnAddress);

        // subscribe to the returnAddress
        SyncSendCallBack cb = new SyncSendCallBack();
        SubscriptionHandler sh = subscribe(connection, returnAddress, cb);
        cb.setSubscriptionHandler(sh);

        publish(connection, x_msg);

        // wait for the response
        int t = 0;
        while (cb.r_msg == null &&
                t < timeOut*1000){
            t++;
            xMsgUtil.sleep(1);
        }
        if(t >= timeOut*1000) {
            throw new TimeoutException("Error: no response for time_out = " + t);
        }
        x_msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());
        return cb.r_msg;
    }

    /**
     * <p>
     *     Subscribes to a specified xMsg topic.
     *     Supplied user callback object must implement xMsgCallBack interface.
     *     This method will de-serialize received xMsgData object and pass it
     *     to the user implemented callback method of the interface.
     *     In the case the request is async the method will
     *     utilize private thread pool to run user callback method in a separate thread.
     * </p>
     * @param connection socket to a xMsgNode proxy output port.
     * @param topic topic of the subscription
     * @param cb {@link xMsgCallBack} implemented object reference
     * @return SubscriptionHandler object reference
     * @throws xMsgException
     */
    public SubscriptionHandler subscribe(final xMsgConnection connection,
                                         String topic,
                                         final xMsgCallBack cb)
            throws xMsgException {

        // check connection
        final Socket con = connection.getSubSock();
        if (con == null) {
            System.out.println("Error: null connection object");
            throw new xMsgException("Error: null connection object");
        }

        // zmq subscribe
        con.subscribe(topic.getBytes(ZMQ.CHARSET));

        SubscriptionHandler sHandle = new SubscriptionHandler(connection, topic) {
            @Override
            public void handle() throws xMsgException, IOException {
                final xMsgMessage cb_msg;

                ZMsg z_msg = ZMsg.recvMsg(con);
                ZFrame r_topic = z_msg.pop();       // get the topic frame = 1
                ZFrame r_msgLocation = z_msg.pop(); // get the message location frame = 2

                // de-serialize received message components
                String ds_topic = new String(r_topic.getData(), ZMQ.CHARSET);
                String ds_msgLocation = new String(r_msgLocation.getData(), ZMQ.CHARSET);

                // we do not need frames
                r_topic.destroy();
                r_msgLocation.destroy();

                // Define/create received xMsgMessage
                // by check the location of the message
                if (ds_msgLocation.equals("envelope")) {

                    // Get the rest of the message components,
                    // i.e. metadata and data are in the xMsg envelope
                    // Read the rest of the zmq frames
                    ZFrame r_metadata = z_msg.pop(); // get metadata frame = 3
                    ZFrame r_data = z_msg.pop();     // get the data frame = 4

                    // metadata and data de-serialization
                    try {
                        xMsgMeta s_metadata = xMsgMeta.parseFrom(r_metadata.getData());
                        xMsgMeta.Builder ds_metadata = s_metadata.toBuilder();

                        // Find the type of the data passed, and de
                        // serialize only X_Object and J_Object type data.
                        // Note de-serialization of the other type data is assumed
                        // to be a user responsibility
                        if (ds_metadata.getDataType().equals(xMsgMeta.DataType.X_Object)) {

                            // xMsgData de-serialization
                            xMsgData s_data = xMsgData.parseFrom(r_data.getData());
                            xMsgData.Builder ds_data = s_data.toBuilder();

                            // Create a message to be passed to the user callback method
                            cb_msg = new xMsgMessage(ds_topic, ds_metadata, ds_data);
                            // Calling user callback method with the received xMsgMessage
                            _callUserCallBack(connection, cb, cb_msg);

                        } else if (ds_metadata.getDataType().equals(xMsgMeta.DataType.J_Object)) {

                            // Java object de-serialization
                            Object ds_data = xMsgUtil.B2O(r_data.getData());

                            // Create a message to be passed to the user callback method
                            cb_msg = new xMsgMessage(ds_topic, ds_metadata, ds_data);
                            // Calling user callback method with the received xMsgMessage
                            _callUserCallBack(connection, cb, cb_msg);

                        } else {
                            // NO de-serialization
                            Object ds_data = r_data.getData();

                            // Create a message to be passed to the user callback method
                            cb_msg = new xMsgMessage(ds_topic, ds_metadata, ds_data);
                            // Calling user callback method with the received xMsgMessage
                            _callUserCallBack(connection, cb, cb_msg);
                        }

                        // we do not need the rest of frames as well as the z_msg
                        r_metadata.destroy();
                        r_data.destroy();
                        z_msg.destroy();

                    } catch (InvalidProtocolBufferException e) {
                        r_metadata.destroy();
                        r_data.destroy();
                        z_msg.destroy();
                        throw new xMsgException(e.getMessage());
                    }
                } else {
                    cb_msg = xMsgRegistrar._shared_memory.get(ds_msgLocation);

                    // Calling user callback method with the received xMsgMessage
                    _callUserCallBack(connection, cb, cb_msg);
                }
            }
        };

        // wait for messages published to a required topic
        Thread t = new Thread(sHandle);
        t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();
            }
        });
        t.start();
        return sHandle;

    }

    private void _callUserCallBack(final xMsgConnection connection,
                                   final xMsgCallBack cb,
                                   final xMsgMessage cb_msg)
            throws xMsgException, IOException {

        // Check if it is sync request
        // sync request
        if (!cb_msg.getMetaData().getReplyTo().equals(xMsgConstants.UNDEFINED.getStringValue())) {
            xMsgMessage rm = cb.callback(cb_msg);
            if (rm != null) {
                rm.setTopic(cb_msg.getMetaData().getReplyTo());
                publish(connection, rm);
            }
        } else {
            // async request
            threadPool.submit(new Runnable() {
                                  public void run() {
                                      cb.callback(cb_msg);
                                  }
                              }
            );
        }
    }

    /**
     * <p>
     *     Un-subscribes  subscription. This will stop
     *     thread and perform xmq un-subscribe
     * </p>
     * @param handle SubscribeHandler object reference
     * @throws xMsgException
     */
    public void unsubscribe(SubscriptionHandler handle)
            throws xMsgException {
        handle.unsubscribe();
    }



    /**
     * <p>
     *     Returns internal thread pool size
     * </p>
     * @return size of the thread pool
     */
    public int getPool_size() {
        return pool_size;
    }

    /**
     * <p>
     *     Private inner class used to organize
     *     sync send/publish communications
     * </p>
     */
    private class SyncSendCallBack implements xMsgCallBack {

        public xMsgMessage r_msg = null;
        public Boolean isReceived  =false;

        private SubscriptionHandler sh = null;

        public void setSubscriptionHandler(SubscriptionHandler sh){
            this.sh = sh;
        }

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
            isReceived = true;
            r_msg = msg;
            try {
                if(sh!=null) {
                    unsubscribe(sh);
                }
            } catch (xMsgException e) {
                e.printStackTrace();
            }

            return r_msg;
        }
    }


    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new FixedExecutor(nThreads, nThreads,
                                      0L, TimeUnit.MILLISECONDS,
                                      new LinkedBlockingQueue<Runnable>());
    }


    private static class FixedExecutor extends ThreadPoolExecutor {

        public FixedExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                TimeUnit unit, BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            if (t == null && r instanceof Future<?>) {
                try {
                    Future<?> future = (Future<?>) r;
                    if (future.isDone()) {
                        future.get();
                    }
                } catch (CancellationException ce) {
                    t = ce;
                } catch (ExecutionException ee) {
                    t = ee.getCause();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // ignore/reset
                }
            }
            if (t != null) {
                t.printStackTrace();
            }
        }
    }
}
