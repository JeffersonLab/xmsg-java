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
import org.jlab.coda.xmsg.excp.xMsgDiscoverException;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**
 * Methods for registration and discovery of xMsg actors, i.e. publishers and
 * subscribers.
 * <p>
 * This class also contains the base method used by all xMsg extending classes
 * to create 0MQ socket for communications. This means that this class owns the
 * 0MQ context.
 * The sockets use the default registrar port: {@link xMsgConstants#REGISTRAR_PORT}.
 * <br>
 * TODO: in the future this class may allow custom port number.
 *
 * @author gurjyan
 * @since 1.0
 */
// CHECKSTYLE.OFF: MethodName
public class xMsgRegDiscDriver {

    /** Front-end registrar server (req/rep) connection socket. */
    private final Socket _feConnection;

    /** Local registrar server (req/rep) connection socket. */
    private final Socket _lnConnection;

    /** zmq context. */
    private final ZContext _context;


    /**
     * Class constructor.
     * Creates sockets to both front-end and local registration and discovery
     * servers. Uses default port.
     *
     * @param feHost the hostname of the front-end
     * @throws SocketException if an I/O error occurs.
     * @throws xMsgException if the host IP address could not be obtained.
     */
    public xMsgRegDiscDriver(String feHost) throws SocketException, xMsgException {

        _context = new ZContext();

        _feConnection = __zmqSocket(_context, ZMQ.REQ,
                xMsgUtil.toHostAddress(feHost),
                xMsgConstants.REGISTRAR_PORT.getIntValue(),
                xMsgConstants.CONNECT.getIntValue());

        if (!feHost.equals("localhost")) {
            _lnConnection = __zmqSocket(_context, ZMQ.REQ,
                    xMsgUtil.toHostAddress("localhost"),
                    xMsgConstants.REGISTRAR_PORT.getIntValue(),
                    xMsgConstants.CONNECT.getIntValue());
        } else {
            _lnConnection = _feConnection;
        }
    }

    /**
     * Creates and returns 0MQ socket.
     *
     * @param context the common 0MQ context
     * @param socketType the type of the socket (integer defined by 0MQ)
     * @param host host name
     * @param port port number
     * @param boc if set 0 socket will be bind, otherwise it will connect.
     *                Note that for xMsg proxies we always connect (boc = 1)
     *                (proxies are XPUB/XSUB sockets).
     * @return zmq socket object
     * @throws org.jlab.coda.xmsg.excp.xMsgException
     */
    public static Socket __zmqSocket(ZContext context,
                                     int socketType,
                                     String host,
                                     int port,
                                     int boc)
            throws xMsgException {

        // Create a zmq socket
        Socket sb = context.createSocket(socketType);
        if (sb == null) {
            throw new xMsgException("null zmq socket-base");
        }

        if (boc == xMsgConstants.BIND.getIntValue()) {
            // Bind socket to the host and port
            int bindPort = sb.bind("tcp://" + host + ":" + port);
            if (bindPort <= 0) {
                throw new xMsgException("can not bind to the port = " + bindPort);
            }
        } else if (boc == xMsgConstants.CONNECT.getIntValue()) {
            // Connect to the host and port
            sb.connect("tcp://" + host + ":" + port);
        } else {
            throw new xMsgException("unknown socket bind/connect option");
        }
        return sb;
    }

    /**
     * Returns the main 0MQ context.
     */
    public ZContext getContext() {
        return _context;
    }

    /**
     * Sends a registration request to the given registrar server.
     * Request is wired using xMsg message construct, that have 3 part: topic,
     * sender, and data.
     * Data is a {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} object.
     *
     * @param socket socket to the local or front-end registration server
     * @param name the name of the sender
     * @param data the registration data object
     * @param isPublisher if true then this is a request to register a publisher,
     *                     otherwise this is a request to register a subscriber
     * @throws xMsgRegistrationException
     */
    private void _register(Socket socket,
                           String name,
                           xMsgRegistration data,
                           boolean isPublisher)
            throws xMsgRegistrationException {

        // Byte array for the registration data serialization
        byte[] dt;

        // Data serialization
        if (data.isInitialized()) {
            dt = data.toByteArray(); // serialize data object
        } else  {
            throw new xMsgRegistrationException("The registration data is not complete");
        }

        // Send topic, sender, followed by the data
        ZMsg msg = new ZMsg();

        // Topic of the message is a string = "registerPublisher" or "registerSubscriber"
        if (isPublisher) {
            msg.addString(xMsgConstants.REGISTER_PUBLISHER.getStringValue());
        } else {
            msg.addString(xMsgConstants.REGISTER_SUBSCRIBER.getStringValue());
        }

        // Sender
        msg.addString(name);

        // Serialized data
        msg.add(dt);

        if (!msg.send(socket)) {
            throw new xMsgRegistrationException("error sending the message");
        }

        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem[] items = {new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.REGISTER_REQUEST_TIMEOUT.getIntValue());


        if (rc != -1 && items[0].isReadable()) {
            //Server responded. It is alive

            // Block until we receive the answer to our registration request
            ZMsg repMsg = ZMsg.recvMsg(socket);

            // get serialized content of the message that is back
            ZFrame repTopicFrame = repMsg.pop();
            ZFrame repSenderFrame = repMsg.pop();
            ZFrame repDataFrame = repMsg.pop();
            if (repDataFrame == null) {
                throw new xMsgRegistrationException("null xMsg data is received");
            }

            // De-serialize received message components
            String repData = new String(repDataFrame.getData(), ZMQ.CHARSET);

            // cleanup
            repTopicFrame.destroy();
            repSenderFrame.destroy();
            repDataFrame.destroy();
            repMsg.destroy();

            // data sent back from the registration server should be a string = "success"
            if (!repData.equals(xMsgConstants.SUCCESS.getStringValue())) {
                throw new xMsgRegistrationException("failed");
            }
            System.out.println(xMsgUtil.currentTime(2) + " " + name + " successfully registered.");

        } else {
            throw new xMsgRegistrationException("xMsg actor registration timeout");
        }
    }

    /**
     * Sends a registration request to the given registrar server.
     * Request is wired using xMsg message construct, that have 3 part: topic,
     * sender, and data.
     * Data is a {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} object.
     *
     * @param socket socket to the local or front-end registration server
     * @param name the name of the sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration} object
     * @param data the registration data object
     * @param isPublisher if true then this is a request to remove a publisher,
     *                     otherwise this is a request to remove a subscriber
     * @throws xMsgRegistrationException
     */
    private void _removeRegistration(Socket socket,
                                     String name,
                                     xMsgRegistration data,
                                     boolean isPublisher)
            throws xMsgRegistrationException {

        // Byte array for the registration data serialization
        byte[] dt;

        // Data serialization
        if (data.isInitialized()) {
            dt = data.toByteArray(); // serialize data object
            if (dt == null) {
                throw new xMsgRegistrationException("null serialization: data");
            }
        } else {
            throw new xMsgRegistrationException("The registration data is not complete");
        }

        // Send topic, sender, followed by the data
        ZMsg msg = new ZMsg();

        // Topic of the message is a string
        // "removePublisherRegistration" or "removeSubscriberRegistration"
        if (isPublisher) {
            msg.addString(xMsgConstants.REMOVE_PUBLISHER.getStringValue());
        } else {
            msg.addString(xMsgConstants.REMOVE_SUBSCRIBER.getStringValue());
        }

        // Sender
        msg.addString(name);

        // Serialized data
        msg.add(dt);

        if (!msg.send(socket)) {
            throw new xMsgRegistrationException("error sending the message");
        }

        // destroy message
        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem[] items = {new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue());

        if (rc != -1 && items[0].isReadable()) {
            //Server responded. It is alive

            // Block until we receive the answer to our registration request
            ZMsg repMsg = ZMsg.recvMsg(socket);

            // get serialized content of the message that is back
            ZFrame repTopicFrame = repMsg.pop();
            ZFrame repSenderFrame = repMsg.pop();
            ZFrame repDataFrame = repMsg.pop();
            if (repDataFrame == null) {
                throw new xMsgRegistrationException("null xMsg data is received");
            }

            // De-serialize received message components
            String repData = new String(repSenderFrame.getData(), ZMQ.CHARSET);

            // cleanup
            repTopicFrame.destroy();
            repSenderFrame.destroy();
            repDataFrame.destroy();
            repMsg.destroy();

            // data sent back from the registration server should a string = "success"
            if (!repData.equals(xMsgConstants.SUCCESS.getStringValue())) {
                throw new xMsgRegistrationException("failed");
            }
        } else {
            throw new xMsgRegistrationException("xMsg actor remove registration timeout");
        }
    }

    /**
     * Removes registration of all xMsg actors of the specified node.
     * This will remove all publishers and subscribers from the global
     * registration database in the front-end that were previously registered on
     * the local database of the specified host.
     * This method is usually called by the xMsgNode registrar when
     * it shuts down or gets interrupted.
     *
     * @param host host name of the xMsgNode
     * @param name the name of the sender
     * @throws xMsgRegistrationException
     */
    public void removeAllRegistrationFE(String host,
                                        String name)
            throws xMsgRegistrationException {

        // Send topic, sender, followed by the data
        ZMsg msg = new ZMsg();

        // Topic of the message is a string =  "removeAllRegistration"
        msg.addString(xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue());

        // Sender
        msg.addString(name);

        // host of the xMsg node
        msg.addString(host);

        if (!msg.send(_feConnection)) {
            throw new xMsgRegistrationException("error sending the message");
        }

        // destroy message
        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem[] items = {new ZMQ.PollItem(_feConnection, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue());

        if (rc != -1 && items[0].isReadable()) {
            //Server responded. It is alive

            // Block until we receive the answer to our registration request
            ZMsg repMsg = ZMsg.recvMsg(_feConnection);

            // get serialized content of the message that is back
            ZFrame repTopicFrame = repMsg.pop();
            ZFrame repSenderFrame = repMsg.pop();
            ZFrame repDataFrame = repMsg.pop();
            if (repDataFrame == null) {
                throw new xMsgRegistrationException("null xMsg data is received");
            }

            // De-serialize received message components
            String repData = new String(repSenderFrame.getData(), ZMQ.CHARSET);

            // cleanup
            repTopicFrame.destroy();
            repSenderFrame.destroy();
            repDataFrame.destroy();
            repMsg.destroy();

            // data sent back from the registration server should a string = "success"
            if (!repData.equals(xMsgConstants.SUCCESS.getStringValue())) {
                throw new xMsgRegistrationException("failed");
            }
        } else {
            throw new xMsgRegistrationException("xMsg actor remove registration timeout");
        }
    }

    /**
     * Searches a registration database for the given topic.
     * This will search the database defined by the specified socket, for
     * publishers or subscribers to the required topic.
     * The xMsg topic components are defined within the registration data.
     *
     * @param socket socket to the local or front-end registration server
     * @param name the name of the sender
     * @param data the registration data object
     * @param isPublisher if true then this is a request to find publishers,
     *                     otherwise this is a request to find subscribers
     * @return list of publishers or subscribers to the required topic
     * @throws xMsgDiscoverException
     */
    private List<xMsgRegistration> _find(Socket socket,
                                         String name,
                                         xMsgRegistration data,
                                         boolean isPublisher)
            throws xMsgDiscoverException {

        // Byte array for the registration data serialization
        byte[] dt;

        // Data serialization
        if (data.isInitialized()) {
            dt = data.toByteArray(); // serialize data object
        } else {
            throw new xMsgDiscoverException("The registration data is not complete");
        }

        // Send topic, sender, followed by the data
        ZMsg msg = new ZMsg();

        // Topic of the message is a string = "findPublisher" or "findSubscriber"
        if (isPublisher) {
            msg.addString(xMsgConstants.FIND_PUBLISHER.getStringValue());
        } else {
            msg.addString(xMsgConstants.FIND_SUBSCRIBER.getStringValue());
        }

        // Sender
        msg.addString(name);

        // serialized data
        msg.add(dt);

        // Sending the request
        if (!msg.send(socket)) {
            throw new xMsgDiscoverException("error sending the data");
        }

        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem[] items = {new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.FIND_REQUEST_TIMEOUT.getIntValue());

        if (rc != -1 && items[0].isReadable()) {

            // Block until we receive the message
            ZMsg repMsg = ZMsg.recvMsg(socket);
            ZFrame repTopicFrame = repMsg.pop();
            ZFrame repSenderFrame = repMsg.pop();

            List<xMsgRegistration> res = new ArrayList<>();
            while (!repMsg.isEmpty()) {
                ZFrame repDataFrame = repMsg.pop();
                if (repDataFrame == null) {
                    throw new xMsgDiscoverException("null xMsg data is received");
                }
                try {
                    res.add(xMsgRegistration.parseFrom(repDataFrame.getData()));
                } catch (InvalidProtocolBufferException e) {
                    throw new xMsgDiscoverException(e.getMessage());
                }
                repDataFrame.destroy();
            }

            // cleanup
            repTopicFrame.destroy();
            repSenderFrame.destroy();
            repMsg.destroy();
            return res;
        } else {
            throw new xMsgDiscoverException("xMsg actor discovery timeout");
        }
    }

    /**
     * Registers xMsg actor with the front-end registration and discovery server.
     *
     * @param name the name of the requester/sender
     * @param data the registration data of the actor
     * @param isPublisher if true this is a request to register a publisher,
     *                    otherwise this is a request to register a subscriber
     * @throws xMsgRegistrationException
     */
    public void registerFrontEnd(String name,
                                 xMsgRegistration data,
                                 boolean isPublisher)
            throws xMsgRegistrationException {
        _register(_feConnection, name, data, isPublisher);
    }

    /**
     * Registers xMsg actor with the local registration and discovery server.
     *
     * @param name the name of the requester/sender
     * @param data the registration data of the actor
     * @param isPublisher if true this is a request to register a publisher,
     *                    otherwise this is a request to register a subscriber
     * @throws xMsgRegistrationException
     */
    public void registerLocal(String name,
                              xMsgRegistration data,
                              boolean isPublisher)
            throws xMsgRegistrationException {
        _register(_lnConnection, name, data, isPublisher);
    }

    /**
     * Removes xMsg actor from the front-end registration and discovery server.
     *
     * @param name the name of the requester/sender
     * @param data the registration data of the actor
     * @param isPublisher if true this is a request to remove a publisher,
     *                    otherwise this is a request to remove a subscriber
     * @throws xMsgRegistrationException
     */
    public void removeRegistrationFrontEnd(String name,
                                           xMsgRegistration data,
                                           boolean isPublisher)
            throws xMsgRegistrationException {
        _removeRegistration(_feConnection, name, data, isPublisher);
    }

    /**
     * Removes xMsg actor from the local registration and discovery server.
     *
     * @param name the name of the requester/sender
     * @param data the registration data of the actor
     * @param isPublisher if true this is a request to remove a publisher,
     *                    otherwise this is a request to remove a subscriber
     * @throws xMsgRegistrationException
     */
    public void removeRegistrationLocal(String name,
                                        xMsgRegistration data,
                                        boolean isPublisher)
            throws xMsgRegistrationException {
        _removeRegistration(_lnConnection, name, data, isPublisher);
    }

    /**
     * Searches the local registration database for the given topic.
     * This will search the local registration and discovery database, for
     * publishers or subscribers to the required topic.
     * The xMsg topic components are defined within the registration data.
     *
     * @param name the name of the sender
     * @param data the registration data object
     * @param isPublisher if true then this is a request to find publishers,
     *                     otherwise this is a request to find subscribers
     * @return list of publishers or subscribers to the required topic
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistration> findLocal(String name,
                                            xMsgRegistration data,
                                            boolean isPublisher)
            throws xMsgDiscoverException {
        return _find(_lnConnection, name, data, isPublisher);
    }

    /**
     * Searches the front-end registration database for the given topic.
     * This will search the local registration and discovery database, for
     * publishers or subscribers to the required topic.
     * The xMsg topic components are defined within the registration data.
     *
     * @param name the name of the sender
     * @param data the registration data object
     * @param isPublisher if true then this is a request to find publishers,
     *                     otherwise this is a request to find subscribers
     * @return list of publishers or subscribers to the required topic
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistration> findGlobal(String name,
                                             xMsgRegistration data,
                                             boolean isPublisher)
            throws xMsgDiscoverException {
        return _find(_feConnection, name, data, isPublisher);
    }
}
