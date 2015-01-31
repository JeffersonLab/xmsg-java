package org.jlab.coda.xmsg.core;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.excp.*;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDiscDriver;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    public xMsg(String feHost) throws xMsgException {
        /**
         * <p>
         *     Calls xMsgRegDiscDriver class constructor that creates sockets to
         *     2 registrar request/reply servers running in the local
         *     xMsgNode and xMsgFE.
         * </p>
         */
        driver = new xMsgRegDiscDriver(feHost);
        _context = driver.getContext();

        threadPool = Executors.newFixedThreadPool(this.pool_size);
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
    public xMsg(String feHost, int pool_size) throws xMsgException {
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
        threadPool = Executors.newFixedThreadPool(this.pool_size);
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
    public xMsgConnection connect() throws xMsgException {
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
    public xMsgConnection connect(String host) throws xMsgException {
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
    public xMsgConnection connect(String host, int port) throws xMsgException {
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
     * @throws xMsgRegistrationException
     */
    public void registerSubscriber(String name,
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
        regb.setOwnerType(xMsgRegistrationData.OwnerType.SUBSCRIBER);
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
     * @throws xMsgRegistrationException
     */
    public void registerSubscriber(String name,
                                   String domain,
                                   String subject,
                                   String type)
            throws xMsgRegistrationException {
        registerSubscriber(name, "localhost", xMsgConstants.DEFAULT_PORT.getIntValue(),
                domain, subject, type);
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
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
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
     * @return list of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
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
     * @param publisherName the name of the publisher/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param data String object
     * @throws xMsgPublishingException
     */
    public void publish_str(xMsgConnection connection,
                            String domain,
                            String subject,
                            String type,
                            String publisherName,
                            String data)
            throws xMsgException {

        // check connection
        Socket con = connection.getPubSock();
        if (con==null) throw new xMsgPublishingException("null connection object");

        // byte array for holding the serialized data object
        byte[] dt;

        // build a topic
        String topic = xMsgUtil.buildTopic(domain, subject, type);


        // send topic, sender, followed by the data
        ZMsg msg = new ZMsg();
        msg.addString(topic);
        msg.addString(publisherName);

        if(data!=null) {
            msg.add(data);
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
     * @param publisherName the name of the publisher/sender. Required according to
     *             the xMsg zmq message structure definition. (topic, sender, data)
     * @param data {@link org.jlab.coda.xmsg.data.xMsgD.Data} object
     * @throws xMsgPublishingException
     */
    private void _publish(xMsgConnection connection,
                          String domain,
                          String subject,
                          String type,
                          String publisherName,
                          xMsgD.Data data)
            throws xMsgException {

        // check connection
        Socket con = connection.getPubSock();
        if (con==null) throw new xMsgPublishingException("null connection object");

        // byte array for holding the serialized data object
        byte[] dt;

        // build a topic
        String topic = xMsgUtil.buildTopic(domain, subject, type);


        // send topic, sender, followed by the data
        ZMsg msg = new ZMsg();
        msg.addString(topic);
        msg.addString(publisherName);

        if(data!=null) {
            // data serialization
            if (data.isInitialized()) {
                dt = data.toByteArray(); // serialize data object
            } else throw new xMsgPublishingException("some of the data object " +
                    "required fields are not set.");
            msg.add(dt);
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
     * @param msg {@link xMsgMessage} object
     * @throws xMsgPublishingException
     */
    public void publish(xMsgConnection connection, xMsgMessage msg) throws xMsgException {
        _publish(connection,
                msg.getDomain(),
                msg.getSubject(),
                msg.getType(),
                msg.getAuthor(),
                msg.getData());
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
     * @param isSync if set to true method will block until subscription method is
     *               received and user callback method is returned
     */
    public void subscribe(final xMsgConnection connection,
                          final String domain,
                          final String subject,
                          final String type,
                          final xMsgCallBack cb,
                          final boolean isSync) {
        try {
            _subscribe(connection, domain, subject, type, cb, isSync);
        } catch (xMsgException e) {
            e.printStackTrace();
        }
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
     *     In the case isSync input parameter is set to be false the method will
     *     utilize private thread pool to run user callback method in a separate thread.
     * </p>
     * @param connection socket to a xMsgNode proxy output port.
     * @param domain domain of the topic
     * @param subject subject of the topic
     * @param type type of the topic
     * @param cb {@link xMsgCallBack} implemented object reference
     * @param isSync if set to true method will block until subscription method is
     *               received and user callback method is returned
     * @throws xMsgSubscribingException
     */
    public void subscribe_str(xMsgConnection connection,
                              String domain,
                              String subject,
                              String type,
                              final xMsgCallBack cb,
                              boolean isSync)
            throws xMsgException {

        // check connection
        Socket con = connection.getSubSock();
        if (con==null) throw new xMsgSubscribingException("null connection object");

        // subscribe the topic
        String topic = xMsgUtil.buildTopic(domain, subject, type);
        con.subscribe(topic.getBytes(ZMQ.CHARSET));

        // wait for messages published to a required topic
        while (!Thread.currentThread().isInterrupted()) {

            ZMsg msg = ZMsg.recvMsg(con);
            ZFrame r_topic = msg.pop();
            ZFrame r_sender = msg.pop();
            ZFrame r_data = msg.pop();

            // de-serialize received message components
            String s_topic = new String(r_topic.getData(), ZMQ.CHARSET);
            String s_sender = new String(r_sender.getData(),ZMQ.CHARSET);
            String s_data = new String(r_sender.getData(),ZMQ.CHARSET);

            // cleanup the data
            r_data.destroy();

            // cleanup the rest
            r_topic.destroy();
            r_sender.destroy();
            msg.destroy();

            // Create a message to be passed to the user callback method
            final xMsgMessage cb_msg = new xMsgMessage(s_sender,
                    xMsgUtil.getTopicDomain(s_topic),
                    xMsgUtil.getTopicSubject(s_topic),
                    xMsgUtil.getTopicType(s_topic),
                    s_data);

            // Calling user callback method
            if(isSync){
                cb.callback(cb_msg);
            } else {
                threadPool.submit(new Runnable() {
                                      public void run() {
                                          cb.callback(cb_msg);
                                      }
                                  }
                );
            }
        }
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
     * @param isSync if set to true method will block until subscription method is
     *               received and user callback method is returned
     * @throws xMsgSubscribingException
     */
    private void _subscribe(xMsgConnection connection,
                            String domain,
                            String subject,
                            String type,
                            final xMsgCallBack cb,
                            boolean isSync)
            throws xMsgException {

        // check connection
        Socket con = connection.getSubSock();
        if (con==null) throw new xMsgSubscribingException("null connection object");

        // subscribe the topic
        String topic = xMsgUtil.buildTopic(domain, subject, type);
        con.subscribe(topic.getBytes(ZMQ.CHARSET));

        // wait for messages published to a required topic
        while (!Thread.currentThread().isInterrupted()) {

            ZMsg msg = ZMsg.recvMsg(con);
            ZFrame r_topic = msg.pop();
            ZFrame r_sender = msg.pop();
            ZFrame r_data = msg.pop();

            // de-serialize received message components
            String s_topic = new String(r_topic.getData(), ZMQ.CHARSET);
            String s_sender = new String(r_sender.getData(),ZMQ.CHARSET);

            xMsgD.Data s_data = null;
            if(r_data != null) {
                try {
                    s_data = xMsgD.Data.parseFrom(r_data.getData());
                } catch (InvalidProtocolBufferException e) {
                    throw new xMsgSubscribingException(e.getMessage());
                }
                // cleanup the data
                r_data.destroy();
            }

            // cleanup the rest
            r_topic.destroy();
            r_sender.destroy();
            msg.destroy();

            // Create a message to be passed to the user callback method
            final xMsgMessage cb_msg = new xMsgMessage(s_sender,
                    xMsgUtil.getTopicDomain(s_topic),
                    xMsgUtil.getTopicSubject(s_topic),
                    xMsgUtil.getTopicType(s_topic),
                    s_data);

            // Calling user callback method
            if(isSync){
                cb.callback(cb_msg);
            } else {
                threadPool.submit(new Runnable() {
                                      public void run() {
                                          cb.callback(cb_msg);
                                      }
                                  }
                );
            }
        }
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

}
