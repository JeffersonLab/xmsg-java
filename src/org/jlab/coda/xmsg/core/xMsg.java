package org.jlab.coda.xmsg.core;

import com.google.protobuf.InvalidProtocolBufferException;

import org.jlab.coda.xmsg.excp.xMsgDiscoverException;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgPublishingException;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.excp.xMsgSubscribingException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDiscDriver;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 *     (running within {@link org.jlab.coda.xmsg.xsys.xMsgNode})
 *     and/or global (running within {@link org.jlab.coda.xmsg.xsys.xMsgFE})
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
        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.PUBLISHER);
        xMsgRegistrationData r_data = regb.build();
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
        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.SUBSCRIBER);
        regb.setDescription(description);
        xMsgRegistrationData r_data = regb.build();
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
        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.PUBLISHER);
        xMsgRegistrationData r_data = regb.build();
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
        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.SUBSCRIBER);
        xMsgRegistrationData r_data = regb.build();
        driver.removeRegistration_local(name, r_data, false);
        driver.removeRegistration_fe(name, r_data, false);
    }

    /**
     * <p>
     *     Finds all local publishers, publishing  to a specified topic
     *     Note: xMsg defines a topic as domain:subject:type
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findLocalPublishers(String name,
                                                          String host,
                                                          int port,
                                                          String domain,
                                                          String subject,
                                                          String type)
            throws xMsgDiscoverException {

        if(domain.equals("*")) throw new xMsgDiscoverException("malformed xMsg topic");
        if(subject.equals("*")) subject = xMsgConstants.UNDEFINED.getStringValue();
        if(type.equals("*")) type = xMsgConstants.UNDEFINED.getStringValue();

        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.PUBLISHER);
        xMsgRegistrationData r_data = regb.build();
        return driver.findLocal(name, r_data, true);
    }

    /**
     * <p>
     *     Finds all local publishers, publishing  to a specified topic
     *     Note: xMsg defines a topic as domain:subject:type
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findLocalPublishers(String name,
                                                          String domain,
                                                          String subject,
                                                          String type)
            throws xMsgDiscoverException {
        return findLocalPublishers(name, "localhost", xMsgConstants.DEFAULT_PORT.getIntValue(),
                domain, subject, type);
    }

    /**
     * <p>
     *     Finds all local subscribers, subscribing  to a specified topic
     *     Note: xMsg defines a topic as domain:subject:type
     * </p>
     * @param name the name of the requester/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param host host of the xMsg node where the caller is running
     * @param port port of the xMsg node where the caller is running
     * @param domain domain of the xMsg subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findLocalSubscribers(String name,
                                                           String host,
                                                           int port,
                                                           String domain,
                                                           String subject,
                                                           String type)
            throws xMsgDiscoverException {

        if(domain.equals("*")) throw new xMsgDiscoverException("malformed xMsg topic");
        if(subject.equals("*")) subject = xMsgConstants.UNDEFINED.getStringValue();
        if(type.equals("*")) type = xMsgConstants.UNDEFINED.getStringValue();

        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.SUBSCRIBER);
        xMsgRegistrationData r_data = regb.build();
        return driver.findLocal(name, r_data, false);
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
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findLocalSubscribers(String name,
                                                           String domain,
                                                           String subject,
                                                           String type)
            throws xMsgDiscoverException {
        return findLocalSubscribers(name, "localhost", xMsgConstants.DEFAULT_PORT.getIntValue(),
                domain, subject, type);
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
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findPublishers(String name,
                                                     String host,
                                                     int port,
                                                     String domain,
                                                     String subject,
                                                     String type)
            throws xMsgDiscoverException {

        if(domain.equals("*")) throw new xMsgDiscoverException("malformed xMsg topic");
        if(subject.equals("*")) subject = xMsgConstants.UNDEFINED.getStringValue();
        if(type.equals("*")) type = xMsgConstants.UNDEFINED.getStringValue();

        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.PUBLISHER);
        xMsgRegistrationData r_data = regb.build();
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
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findPublishers(String name,
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
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findSubscribers(String name,
                                                      String host,
                                                      int port,
                                                      String domain,
                                                      String subject,
                                                      String type)
            throws xMsgDiscoverException {

        if(domain.equals("*")) throw new xMsgDiscoverException("malformed xMsg topic");
        if(subject.equals("*")) subject = xMsgConstants.UNDEFINED.getStringValue();
        if(type.equals("*")) type = xMsgConstants.UNDEFINED.getStringValue();

        xMsgRegistrationData.Builder regb= xMsgRegistrationData.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistrationData.OwnerType.SUBSCRIBER);
        xMsgRegistrationData r_data = regb.build();
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
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findSubscribers(String name,
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
        return findLocalPublishers(name, host, port, domain, subject, type).size() > 0;
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
        return findLocalPublishers(name, domain, subject, type).size() > 0;
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
        return findLocalSubscribers(name, host, port, domain, subject, type).size() > 0;
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
        return findLocalSubscribers(name, domain, subject, type).size() > 0;
    }

    /**
     * <p>
     *     Publishes data to a specified xMsg topic.
     *     This method will perform input data, i.e. xMsgData object serialization.
     * </p>
     * @param connection xMsgConnection object.
     * @param topic topic of the communication
     * @param d data object
     * @throws xMsgPublishingException
     */
    public void publish(xMsgConnection connection,
                        String topic,
                        Object d)
            throws xMsgException {


        // check connection
        Socket con = connection.getPubSock();
        if (con==null) throw new xMsgPublishingException("null connection object");

        // byte array for holding the serialized data object
        byte[] dt;

        // send topic, sender, followed by the data
        ZMsg msg = new ZMsg();
        msg.addString(topic);

        if(d instanceof xMsgD.Data.Builder) {
            msg.addString(xMsgConstants.ENVELOPE_DATA_TYPE_XMSGDATA.getStringValue());
            xMsgD.Data data = ((xMsgD.Data.Builder) d).build();

            // data serialization
            if (data.isInitialized()) {
                dt = data.toByteArray(); // serialize data object
            } else throw new xMsgPublishingException("some of the data object " +
                    "required fields are not set.");
            msg.add(dt);

        } else if(d instanceof String) {
            String data = (String)d;
            msg.addString(xMsgConstants.ENVELOPE_DATA_TYPE_STRING.getStringValue());
            msg.add(data);
        } else {
            throw new xMsgPublishingException("unsupported data type");
        }
        if (!msg.send(con))throw new xMsgPublishingException("error publishing the message");
        msg.destroy();
    }

    /**
     * <p>
     *     Publishes data to a specified xMsg topic. 3 elements are defining xMsg topic:
     *     <ul>
     *         <li>domain</li>
     *         <li>subject</li>
     *         <li>type</li>
     *     </ul>
     *     Topic is constructed from these elements separated by <b>:</b>
     *     Domain is required , however subject and topic can be set to <b>*</b>.
     *     If subject is set * type will be ignored. Here are examples of
     *     accepted topic definitions:<br>
     *         domain:*:* <br>
     *         domain:subject:*<br>
     *         domain:subject:type<br>
     *     This method will perform input data, i.e. xMsgData object serialization.
     * </p>
     * @param connection xMsgConnection object.
     * @param domain domain of the topic
     * @param subject subject of the topic
     * @param type type of the topic
     * @param d data object
     * @throws xMsgPublishingException
     */
    public void publish(xMsgConnection connection,
                        String domain,
                        String subject,
                        String type,
                        Object d)
            throws xMsgException {

        // build a topic
        String topic = xMsgUtil.buildTopic(domain, subject, type);
        publish(connection, topic, d);
    }

    /**
     * <p>
     *     Publishes data to a specified xMsg topic. 3 elements are defining xMsg topic:
     *     <ul>
     *         <li>domain</li>
     *         <li>subject</li>
     *         <li>type</li>
     *     </ul>
     *     Topic is constructed from these elements separated by <b>:</b>
     *     Domain is required , however subject and topic can be set to <b>*</b>.
     *     If subject is set * type will be ignored. Here are examples of
     *     accepted topic definitions:<br>
     *         domain:*:* <br>
     *         domain:subject:*<br>
     *         domain:subject:type<br>
     *     This method will perform input data, i.e. xMsgData object serialization.
     * </p>
     * @param connection xMsgConnection object.
     * @param msg {@link xMsgMessage} object
     * @throws xMsgPublishingException
     */
    public void publish(xMsgConnection connection, xMsgMessage msg) throws xMsgException {
        publish(connection,
                msg.getDomain(),
                msg.getSubject(),
                msg.getType(),
                msg.getData());
    }

    /**
     * <p>
     *     Sync method waits until receiver/subscriber responds back.
     *     Publishes data to a specified xMsg topic.
     *     This method will perform input data, i.e. xMsgData object serialization.
     * </p>
     * @param connection xMsgConnection object.
     * @param topic topic of the communication
     * @param timeOut int in seconds
     * @param d data object
     * @throws xMsgException
     * @throws TimeoutException
     */
    public Object sync_publish(xMsgConnection connection,
                               String topic,
                               Object d,
                               int timeOut)
            throws xMsgException, TimeoutException {

        // address/topic where the subscriber should send the result
        String returnAddress = "return:"+ (int) (Math.random() * 100.0);

        // check pub connection
        Socket pubcon = connection.getPubSock();
        if (pubcon==null) throw new xMsgPublishingException("null connection object");

        // check sub connection
        Socket subcon = connection.getSubSock();
        if (subcon==null) throw new xMsgSubscribingException("null connection object");

        // subscribe to the returnAddress
        SyncSendCallBack cb = new SyncSendCallBack();
        SubscriptionHandler sh = sync_subscribe(connection, returnAddress, cb);
        cb.setSubscriptionHandler(sh);

        // byte array for holding the serialized data object
        byte[] dt;

        // send topic, sender, followed by the data
        ZMsg msg = new ZMsg();
        msg.addString(topic);

        if(d instanceof xMsgD.Data.Builder) {
            msg.addString(xMsgConstants.ENVELOPE_DATA_TYPE_XMSGDATA.getStringValue());
            xMsgD.Data data = ((xMsgD.Data.Builder) d).build();

            // data serialization
            if (data.isInitialized()) {
                dt = data.toByteArray(); // serialize data object
            } else throw new xMsgPublishingException("some of the data object " +
                    "required fields are not set.");
            msg.add(dt);

        } else if(d instanceof String) {
            String data = (String)d;
            msg.addString(xMsgConstants.ENVELOPE_DATA_TYPE_STRING.getStringValue());
            msg.add(data);
        } else {
            throw new xMsgPublishingException("unsupported data type");
        }

        msg.addString(returnAddress);

        if (!msg.send(pubcon))throw new xMsgPublishingException("error publishing the message");
        msg.destroy();

        // wait for the response
        int t = 0;
        while(cb.s_data==null &&
                t < timeOut*1000){
            t++;
            xMsgUtil.sleep(1);
        }
        if(t >= timeOut*1000) {
            throw new TimeoutException();
        }

        return cb.s_data;
    }

    /**
     * <p>
     *     Subscribes and waits a response from a specified xMsg topic.
     * </p>
     * @param connection xMsgConnection object
     * @param topic topic of the subscription
     * @param cb {@link xMsgCallBack} implemented object reference
     * @throws xMsgSubscribingException
     */
    private SubscriptionHandler sync_subscribe(final xMsgConnection connection,
                                               final String topic,
                                               final xMsgCallBack cb)
            throws xMsgException{

        // check connection
        final Socket con = connection.getSubSock();
        if (con==null) throw new xMsgSubscribingException("null connection object");

        con.subscribe(topic.getBytes(ZMQ.CHARSET));

        SubscriptionHandler sHandle = new SubscriptionHandler(connection, topic) {
            @Override
            public void handle() throws xMsgException {
                ZMsg msg = ZMsg.recvMsg(con);
                ZFrame r_addr = msg.pop();
                ZFrame r_dataType = msg.pop();
                ZFrame r_data = msg.pop();

                String ds_dataType = new String(r_dataType.getData(),ZMQ.CHARSET);

                Object ds_data;
                if(ds_dataType.equals(xMsgConstants.ENVELOPE_DATA_TYPE_STRING.getStringValue())) {
                    ds_data = new String(r_data.getData(), ZMQ.CHARSET);
                    r_data.destroy();
                } else {

                    // de-serialize passed transient data
                    try {
                        xMsgD.Data im_data = xMsgD.Data.parseFrom(r_data.getData());

                        // Create a builder object from immutable de-serialized object.
                        ds_data = xMsgUtil.getPbBuilder(im_data);

                    } catch (InvalidProtocolBufferException e) {
                        throw new xMsgSubscribingException(e.getMessage());
                    }
                    // cleanup the data
                    r_data.destroy();
                }

                // cleanup the rest
                r_addr.destroy();
                r_dataType.destroy();
                msg.destroy();

                // Create a message to be passed to the user callback method
                final xMsgMessage cb_msg = new xMsgMessage();
                cb_msg.setData(ds_data);
                cb_msg.setDataType(ds_dataType);

                cb.callback(cb_msg);

            }
        };

        // wait for messages published to a required topic
        new Thread(sHandle).start();
        return sHandle;
    }

    /**
     * <p>
     *     Sync method waits until receiver/subscriber responds back.
     *     Publishes data to a specified xMsg topic. 3 elements are defining xMsg topic:
     *     <ul>
     *         <li>domain</li>
     *         <li>subject</li>
     *         <li>type</li>
     *     </ul>
     *     Topic is constructed from these elements separated by <b>:</b>
     *     Domain is required , however subject and topic can be set to <b>*</b>.
     *     If subject is set * type will be ignored. Here are examples of
     *     accepted topic definitions:<br>
     *         domain:*:* <br>
     *         domain:subject:*<br>
     *         domain:subject:type<br>
     *     This method will perform input data, i.e. xMsgData object serialization.
     * </p>
     * @param connection xMsgConnection object.
     * @param domain domain of the topic
     * @param subject subject of the topic
     * @param type type of the topic
     * @param timeOut int in seconds
     * @param d data object
     * @throws TimeoutException
     * @throws xMsgPublishingException
     */
    public Object sync_publish(xMsgConnection connection,
                               String domain,
                               String subject,
                               String type,
                               Object d,
                               int timeOut)
            throws xMsgException, TimeoutException {

        // build a topic
        String topic = xMsgUtil.buildTopic(domain, subject, type);
        return  sync_publish(connection, topic, d, timeOut);
    }

    /**
     * <p>
     *     Sync method waits until receiver/subscriber responds back.
     *     Publishes data to a specified xMsg topic. 3 elements are defining xMsg topic:
     *     <ul>
     *         <li>domain</li>
     *         <li>subject</li>
     *         <li>type</li>
     *     </ul>
     *     Topic is constructed from these elements separated by <b>:</b>
     *     Domain is required , however subject and topic can be set to <b>*</b>.
     *     If subject is set * type will be ignored. Here are examples of
     *     accepted topic definitions:<br>
     *         domain:*:* <br>
     *         domain:subject:*<br>
     *         domain:subject:type<br>
     *     This method will perform input data, i.e. xMsgData object serialization.
     * </p>
     * @param connection xMsgConnection object.
     * @param msg {@link xMsgMessage} object
     * @param timeOut int in seconds
     * @throws xMsgException
     * @throws TimeoutException
     */
    public Object sync_publish(xMsgConnection connection,
                               xMsgMessage msg,
                               int timeOut)
            throws xMsgException, TimeoutException {
        return sync_publish(connection,
                msg.getDomain(),
                msg.getSubject(),
                msg.getType(),
                msg.getData(),
                timeOut);
    }


    /**
     * <p>
     *     Subscribes to a specified xMsg topic.
     *     Supplied user callback object must implement xMsgCallBack interface.
     *     This method will de-serialize received xMsgData object and pass it
     *     to the user implemented callback method of thee interface.
     *     In the case isSync input parameter is set to be false the method will
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
        if (con==null) throw new xMsgSubscribingException("null connection object");

        con.subscribe(topic.getBytes(ZMQ.CHARSET));

        SubscriptionHandler sHandle = new SubscriptionHandler(connection, topic) {
            @Override
            public void handle() throws xMsgException {
                    ZMsg msg = ZMsg.recvMsg(con);
                    ZFrame r_topic = msg.pop();
                    ZFrame r_dataType = msg.pop();
                    ZFrame r_data = msg.pop();

                    // Check to see if this is a sync request
                    // Note for the sync request xMsg envelope has 4 components:
                    // topic, dataType, data, sync_return_address
                    String syncReturnAddress = xMsgConstants.UNDEFINED.getStringValue();
                    if(!msg.isEmpty()){
                        ZFrame r_addr = msg.pop();
                        if(r_addr!=null) {
                            syncReturnAddress = new String(r_addr.getData(), ZMQ.CHARSET);
                        }
                    }

                    // de-serialize received message components
                    String ds_topic = new String(r_topic.getData(), ZMQ.CHARSET);
                    String ds_dataType = new String(r_dataType.getData(),ZMQ.CHARSET);

                    Object ds_data;
                    if(ds_dataType.equals(xMsgConstants.ENVELOPE_DATA_TYPE_STRING.getStringValue())) {
                        ds_data = new String(r_data.getData(), ZMQ.CHARSET);
                        r_data.destroy();
                    } else {

                        // de-serialize passed transient data
                        try {
                            xMsgD.Data im_data = xMsgD.Data.parseFrom(r_data.getData());

                            // Create a builder object from immutable de-serialized object.
                            ds_data = xMsgUtil.getPbBuilder(im_data);

                        } catch (InvalidProtocolBufferException e) {
                            throw new xMsgSubscribingException(e.getMessage());
                        }
                        // cleanup the data
                        r_data.destroy();
                    }

                    // cleanup the rest
                    r_topic.destroy();
                    r_dataType.destroy();
                    msg.destroy();

                    // Create a message to be passed to the user callback method
                    final xMsgMessage cb_msg = new xMsgMessage(ds_dataType,
                            xMsgUtil.getTopicDomain(ds_topic),
                            xMsgUtil.getTopicSubject(ds_topic),
                            xMsgUtil.getTopicType(ds_topic),
                            ds_data);

                    // Calling user callback method
                    // if it is sync send back to the result
                    if(!syncReturnAddress.equals(xMsgConstants.UNDEFINED.getStringValue())){
                        cb_msg.setIsSyncRequest(true);
                        cb_msg.setSyncRequesterAddress(syncReturnAddress);
                        Object rd = cb.callback(cb_msg);
                        if(rd!=null) {
                            publish(connection, syncReturnAddress, rd);
                        }
                    } else {
                        threadPool.submit(new Runnable() {
                                              public void run() {
                                                  cb.callback(cb_msg);
                                              }
                                          }
                        );
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

    /**
     * <p>
     *     Subscribes to a specified xMsg topic. 3 elements are defining xMsg topic:
     *     <ul>
     *         <li>domain</li>
     *         <li>subject</li>
     *         <li>type</li>
     *     </ul>
     *     Topic is constructed from these elements separated by <b>:</b>
     *     Domain is required , however subject and topic can be set to <b>*</b>.
     *     If subject is set * type will be ignored. Here are examples of
     *     accepted topic definitions:<br>
     *         domain:*:* <br>
     *         domain:subject:*<br>
     *         domain:subject:type<br>
     *     Supplied user callback object must implement xMsgCallBack interface.
     *     This method will de-serialize received xMsgData object and pass it
     *     to the user implemented callback method of thee interface.
     *     In the case isSync input parameter is set to be false the method will
     *     utilize private thread pool to run user callback method in a separate thread.
     * </p>
     * @param connection socket to a xMsgNode proxy output port.
     * @param domain domain of the topic
     * @param subject subject of the topic
     * @param type type of the topic
     * @param cb {@link xMsgCallBack} implemented object reference
     * @throws xMsgSubscribingException
     */
    public SubscriptionHandler subscribe(xMsgConnection connection,
                          String domain,
                          String subject,
                          String type,
                          final xMsgCallBack cb)
            throws xMsgException {

        String topic = xMsgUtil.buildTopic(domain, subject, type);
        return subscribe(connection, topic, cb);
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

        public Object s_data = null;
        public Boolean isReceived  =false;

        private SubscriptionHandler sh = null;

        public void setSubscriptionHandler(SubscriptionHandler sh){
            this.sh = sh;
        }

        @Override
        public Object callback(xMsgMessage msg) {
            isReceived = true;
            s_data = msg.getData();
            try {
                if(sh!=null) {
                    unsubscribe(sh);
                }
            } catch (xMsgException e) {
                e.printStackTrace();
            }

            return s_data;
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
