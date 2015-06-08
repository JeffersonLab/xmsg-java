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
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.jlab.coda.xmsg.xsys.xMsgRegistrar;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver.__zmqSocket;

/**
 * xMsg base class that provides methods for organizing pub/sub communications.
 *
 * This class provides a local database of xMsgCommunication for publishing
 * and/or subscribing messages without requesting registration information from
 * the local registrar services.
 *
 * This class also provides a thread pool for servicing received messages (as a
 * result of a subscription) in separate threads.
 *
 * @author gurjyan
 * @since 1.0
 */
public class xMsg {

    /** 0MQ context object. */
    private final ZContext context;

    /** Private database of stored connections. */
    private final HashMap<String, xMsgConnection> connections = new HashMap<>();

    /** Fixed size thread pool. */
    private final ExecutorService threadPool;
    /** Default thread pool size. */
    private final int poolSize;

    /** Access to the xMsg registrars. */
    private xMsgRegDriver driver;


    /**
     * Constructor. Requires the name of the front-end host that is used to
     * create a connection to the registrar service running within the xMsgFE.
     * Creates the 0MQ context object and thread pool for servicing received
     * messages in a separate threads.
     *
     * @param feHost host name of the front-end
     * @throws xMsgException
     * @throws SocketException
     */
    public xMsg(String feHost) throws xMsgException, SocketException {
        this(new xMsgRegDriver(feHost), 2);
    }

    /**
     * Constructor. Requires the name of the front-end host that is used to
     * create a connection to the registrar service running within the xMsgFE.
     * Creates the zmq context object and thread pool for servicing received
     * messages in a separate threads.
     *
     * @param feHost host name of the FE
     * @param poolSize thread pool size for servicing subscription callbacks
     * @throws SocketException
     * @throws xMsgException
     */
    public xMsg(String feHost, int poolSize) throws xMsgException, SocketException {
        /*
         * Calls xMsgRegDiscDriver class constructor that creates sockets to two registrar
         * request/reply servers running in the local xMsgNode and xMsgFE.
         */
        this(new xMsgRegDriver(feHost), poolSize);
    }


    xMsg(xMsgRegDriver driver, int poolSize) throws SocketException, xMsgException {
        this.localHostIp = xMsgUtil.toHostAddress("localhost");
        this.context = driver.getContext();
        this.driver = driver;

        // create fixed size thread pool
        this.poolSize = poolSize;
        this.threadPool = newFixedThreadPool(this.poolSize);
    }

    /**
     * Returns the connection to the local xMsg proxy.
     * If the connection is not created yet, it will be created and stored into
     * the cache of connections, and then returned.
     * If there is a connection in the cache, that object will be returned then.
     * The local proxy should be running.
     *
     * @return the {@link xMsgConnection} object to the local proxy
     * @throws SocketException
     * @throws xMsgException
     */
    public xMsgConnection connect() throws xMsgException, SocketException {
        return connect(new xMsgAddress("localhost"));
    }

    /**
     * Returns the connection to the xMsg proxy in the specified host.
     * If the connection is not created yet, it will be created and stored into
     * the cache of connections, and then returned.
     * If there is a connection in the cache, that object will be returned then.
     * The proxy should be running in the host.
     *
     * @param host the name of the host where the xMsg proxy is running
     * @return the {@link xMsgConnection} object to the proxy
     * @throws SocketException
     * @throws xMsgException
     */
    public xMsgConnection connect(String host) throws xMsgException, SocketException {
        return connect(new xMsgAddress(host));
    }

    /**
     * Returns the connection to the xMsg proxy in the specified host.
     * Use the given port to open the sockets.
     * If the connection is not created yet, it will be created and stored into
     * the cache of connections, and then returned.
     * If there is a connection in the cache, that object will be returned then.
     * The proxy should be running in the host.
     *
     * @param host the name of the host where the xMsg proxy is running
     * @param port  xMsg node port number
     * @return the {@link xMsgConnection} object to the proxy
     * @throws SocketException
     * @throws xMsgException
     */
    public xMsgConnection connect(String host, int port) throws xMsgException, SocketException {
        return connect(new xMsgAddress(host, port));
    }

    /**
     * Returns the connection to the xMsg proxy in the specified host.
     * If the connection is not created yet, it will be created and stored into
     * the cache of connections, and then returned.
     * If there is a connection in the cache, that object will be returned then.
     * The proxy should be running in the host.
     *
     * @param address the xMsg address of the host where the xMsg proxy is running
     * @return the {@link xMsgConnection} object to the proxy
     * @throws xMsgException
     */
    public xMsgConnection connect(xMsgAddress address) throws xMsgException {

        /*
         * First check to see if we have already established connection
         * to this address
         */
        if (connections.containsKey(address.getKey())) {
            return connections.get(address.getKey());
        } else {
            /*
             * Otherwise create sockets to the requested address, and store the
             * created connection object for the future use. Return the
             * reference to the connection object
             */
            xMsgConnection feCon = new xMsgConnection();
            feCon.setAddress(address);
            feCon.setPubSock(__zmqSocket(context, ZMQ.PUB, address.getHost(),
                    address.getPort(), xMsgConstants.CONNECT.getIntValue()));

            feCon.setSubSock(__zmqSocket(context, ZMQ.SUB, address.getHost(),
                    address.getPort() + 1, xMsgConstants.CONNECT.getIntValue()));

            connections.put(address.getKey(), feCon);
            return feCon;
        }
    }

    /**
     * Returns a new connection to the xMsg proxy in the specified host.
     * A new connection is always created. This connection is not stored in the
     * cache of connections (which may already contain a connection to the given
     * host). The proxy should be running in the host.
     *
     * @param address the xMsg address of the host where the xMsg proxy is running
     * @return the {@link xMsgConnection} object to the proxy
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
     * Registers a publisher in the local registrar.
     * The publisher should be periodically publishing data.
     * Futures subscribers can use this registration to discover and listen to
     * the published messages.
     * The local registration database is periodically updated to the front-end database.
     *
     * @param name the name of the publisher
     * @param host host of the xMsg proxy where the publisher is running
     * @param port port of the xMsg proxy where the publisher is running
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
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
        xMsgRegistration regData = regb.build();
        driver.registerLocal(name, regData, true);
    }

    /**
     * Registers a local publisher in the local registrar.
     * The publisher should be periodically publishing data.
     * Futures subscribers can use this registration to discover and listen to
     * the published messages.
     * The local registration database is periodically updated to the front-end database.
     * The publisher is expected to be running in the local node.
     *
     * @param name the name of the publisher
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
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
     * Registers a subscriber in the local registrar.
     * The subscriber should be listening for messages of the wanted topic.
     * Future publishers might express an interest to publish data to a a
     * required topic of interest or might publish data only if there are active
     * listeners/subscribers to their published topic.
     * The local registration database is periodically updated to the front-end database.
     *
     * @param name the name of the subscriber.
     * @param host host of the xMsg proxy where the subscriber is running
     * @param port port of the xMsg proxy where the subscriber is running
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @param description a description of the subscription
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
        xMsgRegistration regData = regb.build();
        driver.registerLocal(name, regData, false);
    }

    /**
     * Registers a local subscriber in the local registrar.
     * The subscriber should be listening for messages of the wanted topic.
     * Future publishers might express an interest to publish data to a a
     * required topic of interest or might publish data only if there are active
     * listeners/subscribers to their published topic.
     * The local registration database is periodically updated to the front-end database.
     * The subscriber is expected to be running in the local node.
     *
     * @param name the name of the subscriber.
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @param description a description of the subscription
     * @throws xMsgRegistrationException
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
     * Removes a publisher from both the local and front-end registration databases.
     *
     * @param name the name of the publisher
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
     * @throws xMsgRegistrationException
     */
    public void removePublisherRegistration(String name,
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
        xMsgRegistration regData = regb.build();
        driver.removeRegistrationLocal(name, regData, true);
        driver.removeRegistrationFrontEnd(name, regData, true);
    }

    /**
     * Removes a subscriber from both the local and front-end registration databases.
     *
     * @param name the name of the subscriber
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @throws xMsgRegistrationException
     */
    public void removeSubscriberRegistration(String name,
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
        xMsgRegistration regData = regb.build();
        driver.removeRegistrationLocal(name, regData, false);
        driver.removeRegistrationFrontEnd(name, regData, false);
    }

    /**
     * Finds all publishers of the given topic.
     * The publishers are searched in the front-end registrar, and they could
     * be deployed anywhere in the xMsg cloud of nodes.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param host host of the xMsg node where the sender is running
     * @param port port of the xMsg node where the sender is running
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
     * @return list of {@link xMsgRegistration} objects, one per found publisher
     * @throws xMsgDiscoverException
     */
    public Set<xMsgRegistration> findPublishers(String name,
                                                String host,
                                                int port,
                                                String domain,
                                                String subject,
                                                String type)
            throws xMsgRegistrationException {

        if (domain.equals("*")) {
            throw new xMsgRegistrationException("malformed xMsg topic");
        }
        if (subject.equals("*")) {
            subject = xMsgConstants.UNDEFINED.getStringValue();
        }
        if (type.equals("*")) {
            type = xMsgConstants.UNDEFINED.getStringValue();
        }

        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        xMsgRegistration regData = regb.build();
        return driver.findGlobal(name, regData, true);
    }

    /**
     * Finds all publishers of the given topic.
     * The publishers are searched in the front-end registrar, and they could
     * be deployed anywhere in the xMsg cloud of nodes.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
     * @return list of {@link xMsgRegistration} objects, one per found publisher
     * @throws xMsgDiscoverException
     */
    public Set<xMsgRegistration> findPublishers(String name,
                                                String domain,
                                                String subject,
                                                String type)
            throws xMsgRegistrationException {
        return findPublishers(name, "localhost", xMsgConstants.REGISTRAR_PORT.getIntValue(),
                domain, subject, type);
    }

    /**
     * Finds all subscribers of the given topic.
     * The publishers are searched in the front-end registrar, and they could
     * be deployed anywhere in the xMsg cloud of nodes.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param host host of the xMsg node where the sender is running
     * @param port port of the xMsg node where the sender is running
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link xMsgRegistration} objects, one per found subscribers
     * @throws xMsgDiscoverException
     */
    public Set<xMsgRegistration> findSubscribers(String name,
                                                 String host,
                                                 int port,
                                                 String domain,
                                                 String subject,
                                                 String type)
            throws xMsgRegistrationException {

        if (domain.equals("*")) {
            throw new xMsgRegistrationException("malformed xMsg topic");
        }
        if (subject.equals("*")) {
            subject = xMsgConstants.UNDEFINED.getStringValue();
        }
        if (type.equals("*")) {
            type = xMsgConstants.UNDEFINED.getStringValue();
        }

        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setHost(host);
        regb.setPort(port);
        regb.setDomain(domain);
        regb.setSubject(subject);
        regb.setType(type);
        regb.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        xMsgRegistration regData = regb.build();
        return driver.findGlobal(name, regData, false);
    }

    /**
     * Finds all subscribers of a given topic.
     * The publishers are searched in the front-end registrar, and they could
     * be deployed anywhere in the xMsg cloud of nodes.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return list of {@link xMsgRegistration} objects, one per found subscribers
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findSubscribers(String name,
                                                 String domain,
                                                 String subject,
                                                 String type)
            throws xMsgRegistrationException {
        return findSubscribers(name, "localhost", xMsgConstants.REGISTRAR_PORT.getIntValue(),
                domain, subject, type);
    }

    /**
     * Checks the front-end registrar for a publisher of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param host host of the xMsg node where the sender is running
     * @param port port of the xMsg node where the sender is running
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
     * @return true if at least one publisher is found
     * @throws xMsgRegistrationException
     */
    public boolean isTherePublisher(String name,
                                    String host,
                                    int port,
                                    String domain,
                                    String subject,
                                    String type) throws xMsgRegistrationException {
        return findPublishers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * Checks the front-end registrar for a publisher of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
     * @return true if at least one publisher is found
     * @throws xMsgRegistrationException
     */
    public boolean isTherePublisher(String name,
                                    String domain,
                                    String subject,
                                    String type) throws xMsgRegistrationException {
        return findPublishers(name, domain, subject, type).size() > 0;
    }

    /**
     * Checks the front-end registrar for a subscriber of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param host host of the xMsg node where the sender is running
     * @param port port of the xMsg node where the sender is running
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgRegistrationException
     */
    public boolean isThereSubscriber(String name,
                                     String host,
                                     int port,
                                     String domain,
                                     String subject,
                                     String type) throws xMsgRegistrationException {
        return findSubscribers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * Checks the front-end registrar for a subscriber of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgRegistrationException
     */
    public boolean isThereSubscriber(String name,
                                     String domain,
                                     String subject,
                                     String type) throws xMsgRegistrationException {
        return findSubscribers(name, domain, subject, type).size() > 0;
    }

    /**
     * Checks the local registrar for a publisher of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param host host of the xMsg node where the sender is running
     * @param port port of the xMsg node where the sender is running
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
     * @return true if at least one publisher is found
     * @throws xMsgRegistrationException
     */
    public boolean isThereLocalPublisher(String name,
                                         String host,
                                         int port,
                                         String domain,
                                         String subject,
                                         String type) throws xMsgRegistrationException {
        return findPublishers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * Checks the local registrar for a publisher of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param domain domain of the published messages
     * @param subject subject of the published messages
     * @param type type of the published messages
     * @return true if at least one publisher is found
     * @throws xMsgRegistrationException
     */
    public boolean isThereLocalPublisher(String name,
                                         String domain,
                                         String subject,
                                         String type) throws xMsgRegistrationException {
        return findPublishers(name, domain, subject, type).size() > 0;
    }

    /**
     * Checks the local registrar for a subscriber of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param host host of the xMsg node where the sender is running
     * @param port port of the xMsg node where the sender is running
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgRegistrationException
     */
    public boolean isThereLocalSubscriber(String name,
                                          String host,
                                          int port,
                                          String domain,
                                          String subject,
                                          String type) throws xMsgRegistrationException {
        return findSubscribers(name, host, port, domain, subject, type).size() > 0;
    }

    /**
     * Checks the local registrar for a subscriber of the given topic.
     * <p>
     * Note: xMsg defines a topic as {@code domain:subject:type}.
     *
     * @param name the name of the sender
     * @param domain domain of the subscription
     * @param subject subject of the subscription
     * @param type type of the subscription
     * @return true if at least one subscriber is found
     * @throws xMsgRegistrationException
     */
    public boolean isThereLocalSubscriber(String name,
                                          String domain,
                                          String subject,
                                          String type) throws xMsgRegistrationException {
        return findSubscribers(name, domain, subject, type).size() > 0;
    }

    /**
     * Publishes the given message.
     *
     * @param connection the connection to be used to send the message
     * @param msg the message to be sent
     * @throws IOException if the message could not be sent
     * @throws xMsgException
     */
    public void publish(xMsgConnection connection,
                        xMsgMessage msg)
            throws xMsgException, IOException {

        // check connection
        Socket con = connection.getPubSock();
        if (con == null) {
            System.out.println("Error: null connection object");
            throw new xMsgException("Error: null connection object");
        }

        // check message
        if (msg == null) {
            System.out.println("Error: null message");
            throw new xMsgException("Error: null message");
        }

        // byte array for holding the serialized metadata and data object
        byte[] serialMetadata;
        byte[] serialData;

        // send topic, sender, followed by the metadata and data
        ZMsg outputMsg = new ZMsg();

        // adding topic frame = 1
        outputMsg.addString(msg.getTopic());

        xMsgMeta.Builder metadata = msg.getMetaData();
        if (metadata.getDataType().equals(xMsgMeta.DataType.UNDEFINED)) {
            System.out.println("Error: undefined data type");
            throw new xMsgException("Error: undefined data type");
        }

        // publishing over process or network boundaries
        if (metadata.getIsDataSerialized()) {

            // adding message location frame = 2
            outputMsg.add("envelope");

            // adding metadata frame = 3
            xMsgMeta md = metadata.build();
            serialMetadata = md.toByteArray();
            outputMsg.add(serialMetadata);

            // We serialize here only X_Object and J_object type data.
            // The rest we assume under user responsibility
            if (metadata.getDataType().equals(xMsgMeta.DataType.X_Object)) {
                // Get the data from the xMsgMessage
                xMsgData.Builder data = (xMsgData.Builder) msg.getData();
                // xMsgData serialization
                xMsgData dd = data.build();
                serialData = dd.toByteArray();

            } else if (metadata.getDataType().equals(xMsgMeta.DataType.J_Object)) {
                Object data = msg.getData();
                // Java object serialization
                serialData = xMsgUtil.serializeToBytes(data);

            } else {
                // NO serialization
                serialData = (byte[]) msg.getData();
            }

            // adding data frame = 4
            outputMsg.add(serialData);

        } else {
            // Publishing within the same process (for e.g. DPE). In this case
            // xMsgMessage including the data and the metadata is in the shared memory.
            // Check to see if the location of the object in the shared memory is defined

            // Add the xMsgMessage (including metadata and data) to the shared memory
            // Here we define a unique key for this message in the shared memory, using
            // senders unique name and the communication ID set by the user.
            String key = metadata.getSender() + "_" + metadata.getCommunicationId();
            xMsgRegistrar.sharedMemory.put(key, msg);

            // add message location frame = 2
            outputMsg.add(key);
        }

        if (!outputMsg.send(con)) {
            System.out.println("Error: publishing the message");
            throw new xMsgException("Error: publishing the message");
        }
        outputMsg.destroy();

    }

    /**
     * Publishes the given message and waits for a response.
     *
     * @param connection the connection to be used to send the message
     * @param msg the message to be sent
     * @param timeout the time to wait for a response, in milliseconds
     * @return the response message
     * @throws IOException if the message could not be sent
     * @throws TimeoutException if a response is not received
     * @throws xMsgException
     */
    public xMsgMessage syncPublish(xMsgConnection connection,
                                    xMsgMessage msg,
                                    int timeout)
            throws xMsgException,
            TimeoutException,
            IOException {

        // address/topic where the subscriber should send the result
        String returnAddress = "return:" + (int) (Math.random() * 100.0);

        // set the return address as replyTo in the xMsgMessage
        msg.getMetaData().setReplyTo(returnAddress);

        // subscribe to the returnAddress
        SyncSendCallBack cb = new SyncSendCallBack();
        SubscriptionHandler sh = subscribe(connection, returnAddress, cb);
        cb.setSubscriptionHandler(sh);

        publish(connection, msg);

        // wait for the response
        int t = 0;
        while (cb.recvMsg == null && t < timeout * 1000) {
            t++;
            xMsgUtil.sleep(1);
        }
        if (t >= timeout * 1000) {
            throw new TimeoutException("Error: no response for time_out = " + t);
        }
        msg.getMetaData().setReplyTo(xMsgConstants.UNDEFINED.getStringValue());
        return cb.recvMsg;
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
                final xMsgMessage callbackMsg;

                ZMsg inputMsg = ZMsg.recvMsg(con);
                ZFrame topicFrame = inputMsg.pop();       // get the topic frame = 1
                ZFrame msgLocationFrame = inputMsg.pop(); // get the message location frame = 2

                // de-serialize received message components
                String topic = new String(topicFrame.getData(), ZMQ.CHARSET);
                String msgLocation = new String(msgLocationFrame.getData(), ZMQ.CHARSET);

                // we do not need frames
                topicFrame.destroy();
                msgLocationFrame.destroy();

                // Define/create received xMsgMessage
                // by check the location of the message
                if (msgLocation.equals("envelope")) {

                    // Get the rest of the message components,
                    // i.e. metadata and data are in the xMsg envelope
                    // Read the rest of the zmq frames
                    ZFrame metadataFrame = inputMsg.pop(); // get metadata frame = 3
                    ZFrame dataFrame = inputMsg.pop();     // get the data frame = 4

                    // metadata and data de-serialization
                    try {
                        xMsgMeta metadataObj = xMsgMeta.parseFrom(metadataFrame.getData());
                        xMsgMeta.Builder metadata = metadataObj.toBuilder();

                        // Find the type of the data passed, and de
                        // serialize only X_Object and J_Object type data.
                        // Note de-serialization of the other type data is assumed
                        // to be a user responsibility
                        if (metadata.getDataType().equals(xMsgMeta.DataType.X_Object)) {

                            // xMsgData de-serialization
                            xMsgData dataObj = xMsgData.parseFrom(dataFrame.getData());
                            xMsgData.Builder data = dataObj.toBuilder();

                            // Create a message to be passed to the user callback method
                            callbackMsg = new xMsgMessage(topic, metadata, data);
                            // Calling user callback method with the received xMsgMessage
                            callUserCallBack(connection, cb, callbackMsg);

                        } else if (metadata.getDataType().equals(xMsgMeta.DataType.J_Object)) {

                            // Java object de-serialization
                            Object data = xMsgUtil.deserialize(dataFrame.getData());

                            // Create a message to be passed to the user callback method
                            callbackMsg = new xMsgMessage(topic, metadata, data);
                            // Calling user callback method with the received xMsgMessage
                            callUserCallBack(connection, cb, callbackMsg);

                        } else {
                            // NO de-serialization
                            Object data = dataFrame.getData();

                            // Create a message to be passed to the user callback method
                            callbackMsg = new xMsgMessage(topic, metadata, data);
                            // Calling user callback method with the received xMsgMessage
                            callUserCallBack(connection, cb, callbackMsg);
                        }

                        // we do not need the rest of frames as well as the z_msg
                        metadataFrame.destroy();
                        dataFrame.destroy();
                        inputMsg.destroy();

                    } catch (InvalidProtocolBufferException e) {
                        metadataFrame.destroy();
                        dataFrame.destroy();
                        inputMsg.destroy();
                        throw new xMsgException(e.getMessage());
                    }
                } else {
                    callbackMsg = xMsgRegistrar.sharedMemory.get(msgLocation);

                    // Calling user callback method with the received xMsgMessage
                    callUserCallBack(connection, cb, callbackMsg);
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

    private void callUserCallBack(final xMsgConnection connection,
                                  final xMsgCallBack callback,
                                  final xMsgMessage callbackMsg)
            throws xMsgException, IOException {

        // Check if it is sync request
        // sync request
        String requester = callbackMsg.getMetaData().getReplyTo();
        if (!requester.equals(xMsgConstants.UNDEFINED.getStringValue())) {
            xMsgMessage rm = callback.callback(callbackMsg);
            if (rm != null) {
                rm.setTopic(callbackMsg.getMetaData().getReplyTo());
                publish(connection, rm);
            }
        } else {
            // async request
            threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    callback.callback(callbackMsg);
                }
            });
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
     * Returns the size of the internal thread pool.
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Private inner class used to organize sync send/publish communications.
     */
    private class SyncSendCallBack implements xMsgCallBack {

        public xMsgMessage recvMsg = null;
        public Boolean isReceived = false;

        private SubscriptionHandler handler = null;

        public void setSubscriptionHandler(SubscriptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public xMsgMessage callback(xMsgMessage msg) {
            isReceived = true;
            recvMsg = msg;
            try {
                if (handler != null) {
                    unsubscribe(handler);
                }
            } catch (xMsgException e) {
                e.printStackTrace();
            }

            return recvMsg;
        }
    }


    /**
     * Creates a new {@link xMsg.FixedExecutor}.
     */
    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new FixedExecutor(nThreads, nThreads,
                                      0L, TimeUnit.MILLISECONDS,
                                      new LinkedBlockingQueue<Runnable>());
    }


    /**
     * A thread pool executor that prints the stacktrace of uncaught exceptions.
     */
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
