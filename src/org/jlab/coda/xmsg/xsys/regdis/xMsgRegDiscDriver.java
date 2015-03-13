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
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
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
 * <p>
 *     Class that provides methods for registration
 *     and discovery of xMsg actors, i.e. publishers
 *     and subscribers.
 *     This class contains a basic method used by all
 *     xMsg extending classes to create zmq socket for
 *     communications. This means that this class owns
 *     the zmq context.
 *     Constructor uses default registrar port:
 *     {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
 *
 *     todo: in the future this class may allow custom port number
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgRegDiscDriver {

    // Front-end registrar server (req/rep) connection socket
    private Socket _feConnection;

    // Local registrar server (req/rep) connection socket
    private Socket _lnConnection;

    // zmq context
    private ZContext _context;

    /**
     * <p>
     *     Constructor creates sockets to both front-end and local
     *     registration and discovery servers. Uses default port.
     * </p>
     * @param feHost front-end host name
     * @throws xMsgException
     */
    public xMsgRegDiscDriver(String feHost) throws xMsgException, SocketException {

        _context = new ZContext();

        _feConnection = __zmqSocket(_context, ZMQ.REQ,
                xMsgUtil.host_to_ip(feHost),
                xMsgConstants.REGISTRAR_PORT.getIntValue(),
                xMsgConstants.CONNECT.getIntValue());

        if(!feHost.equals("localhost")) {
            _lnConnection = __zmqSocket(_context, ZMQ.REQ,
                    xMsgUtil.host_to_ip("localhost"),
                    xMsgConstants.REGISTRAR_PORT.getIntValue(),
                    xMsgConstants.CONNECT.getIntValue());
        } else {
            _lnConnection = _feConnection;
        }
    }

    /**
     * Returns the main zmq socket context
     * @return zmq context
     */
    public ZContext getContext(){
        return _context;
    }

    /**
     * <p>
     *     Sends registration request to the server. Request is wired using
     *     xMsg message construct, that have 3 part: topic, sender, and data.
     *     Data is the object of the
     *     {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData}
     * </p>
     * @param _connectionSocket connection socket defines the local or
     *                          front-end registration server
     * @param name the name of the sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to register
     *                    a publisher, otherwise this is a subscriber registration
     *                    request
     * @throws xMsgRegistrationException
     */
    private void _register(Socket _connectionSocket,
                           String name,
                           xMsgRegistrationData data,
                           boolean isPublisher)
            throws xMsgRegistrationException {

        // Byte array for the registration data serialization
        byte[] dt;

        // Data serialization
        if(data.isInitialized()) {
            dt = data.toByteArray(); // serialize data object
        } else throw new xMsgRegistrationException("some of the data object required fields are not set.");

        // Send topic, sender, followed by the data
        ZMsg msg = new ZMsg();

        // Topic of the message is a string = "registerPublisher" or "registerSubscriber"
        if(isPublisher) msg.addString(xMsgConstants.REGISTER_PUBLISHER.getStringValue());
        else msg.addString(xMsgConstants.REGISTER_SUBSCRIBER.getStringValue());

        // Sender
        msg.addString(name);

        // Serialized data
        msg.add(dt);

        if (!msg.send(_connectionSocket))throw new xMsgRegistrationException("error sending the message");

        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem items[] = {new ZMQ.PollItem(_connectionSocket, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.REGISTER_REQUEST_TIMEOUT.getIntValue());


        if (rc != -1 && items[0].isReadable()) {
            //Server responded. It is alive

            // Block until we receive the answer to our registration request
            ZMsg r_msg = ZMsg.recvMsg(_connectionSocket);

            // get serialized content of the message that is back
            ZFrame r_topic = r_msg.pop();
            ZFrame r_sender = r_msg.pop();
            ZFrame r_data = r_msg.pop();
            if (r_data == null) throw new xMsgRegistrationException("null xMsg data is received");

            // De-serialize received message components
            String s_topic = new String(r_topic.getData(), ZMQ.CHARSET);
            String s_sender = new String(r_sender.getData(), ZMQ.CHARSET);
            String s_data = new String(r_data.getData(), ZMQ.CHARSET);

            // cleanup
            r_topic.destroy();
            r_sender.destroy();
            r_data.destroy();
            r_msg.destroy();

            // data sent back from the registration server should be a string = "success"
            if (!s_data.equals(xMsgConstants.SUCCESS.getStringValue())) {
                throw new xMsgRegistrationException("failed");
            }
            System.out.println(xMsgUtil.currentTime(2)+" " + name+" successfully registered.");

        } else {
            throw new xMsgRegistrationException("xMsg actor registration timeout");
        }
    }

    /**
     * <p>
     *     Sends remove registration request to the server. Request is wired using
     *     xMsg message construct, that have 3 part: topic, sender, and data.
     *     Data is the object of the
     *     {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData}
     * </p>
     * @param _connectionSocket connection socket defines the local or
     *                          front-end registration server
     * @param name the name of the sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to remove
     *                    a publisher registration, otherwise removes subscriber registration
     * @throws xMsgRegistrationException
     */
    private void _remove_registration(Socket _connectionSocket,
                                      String name,
                                      xMsgRegistrationData data,
                                      boolean isPublisher)
            throws xMsgRegistrationException {

        // Byte array for the registration data serialization
        byte[] dt;

        // Data serialization
        if(data.isInitialized()) {
            dt = data.toByteArray(); // serialize data object
            if (dt == null) throw new xMsgRegistrationException("null serialization: data");
        } else throw new xMsgRegistrationException("some of the data object required fields are not set.");

        // Send topic, sender, followed by the data
        ZMsg msg = new ZMsg();

        // Topic of the message is a string = "removePublisherRegistration" or "removeSubscriberRegistration"
        if(isPublisher) msg.addString(xMsgConstants.REMOVE_PUBLISHER.getStringValue());
        else msg.addString(xMsgConstants.REMOVE_SUBSCRIBER.getStringValue());

        // Sender
        msg.addString(name);

        // Serialized data
        msg.add(dt);

        if (!msg.send(_connectionSocket))throw new xMsgRegistrationException("error sending the message");

        // destroy message
        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem items[] = {new ZMQ.PollItem(_connectionSocket, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue());

        if (rc != -1 && items[0].isReadable()) {
            //Server responded. It is alive

            // Block until we receive the answer to our registration request
            ZMsg r_msg = ZMsg.recvMsg(_connectionSocket);

            // get serialized content of the message that is back
            ZFrame r_topic = r_msg.pop();
            ZFrame r_sender = r_msg.pop();
            ZFrame r_data = r_msg.pop();
            if (r_data == null) throw new xMsgRegistrationException("null xMsg data is received");

            // De-serialize received message components
            // String s_topic = new String(r_topic.getData(), ZMQ.CHARSET);
            // String s_sender = new String(r_sender.getData(), ZMQ.CHARSET);
            String s_data = new String(r_sender.getData(), ZMQ.CHARSET);

            // cleanup
            r_topic.destroy();
            r_sender.destroy();
            r_data.destroy();
            r_msg.destroy();

            // data sent back from the registration server should a string = "success"
            if (!s_data.equals(xMsgConstants.SUCCESS.getStringValue())) {
                throw new xMsgRegistrationException("failed");
            }
        } else {
            throw new xMsgRegistrationException("xMsg actor remove registration timeout");
        }
    }

    /**
     * <p>
     *     Removes all xMsg actors (publishers and subscribers) registration from
     *     the front-end global registration and discovery database that were
     *     previously registered on a specified host local database. This method
     *     is usually called by the xMsgNode Registrar when it shuts down or gets
     *     interrupted.
     * </p>
     * @param host host name of the xMsgNode
     * @param name the name of the sender
     * @throws xMsgRegistrationException
     */
    public void removeAllRegistration_fe(String host,
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

        if (!msg.send(_feConnection))throw new xMsgRegistrationException("error sending the message");

        // destroy message
        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem items[] = {new ZMQ.PollItem(_feConnection, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue());

        if (rc != -1 && items[0].isReadable()) {
            //Server responded. It is alive

            // Block until we receive the answer to our registration request
            ZMsg r_msg = ZMsg.recvMsg(_feConnection);

            // get serialized content of the message that is back
            ZFrame r_topic = r_msg.pop();
            ZFrame r_sender = r_msg.pop();
            ZFrame r_data = r_msg.pop();
            if (r_data == null) throw new xMsgRegistrationException("null xMsg data is received");

            // De-serialize received message components
            // String s_topic = new String(r_topic.getData(), ZMQ.CHARSET);
            // String s_sender = new String(r_sender.getData(), ZMQ.CHARSET);
            String s_data = new String(r_sender.getData(), ZMQ.CHARSET);

            // cleanup
            r_topic.destroy();
            r_sender.destroy();
            r_data.destroy();
            r_msg.destroy();

            // data sent back from the registration server should a string = "success"
            if (!s_data.equals(xMsgConstants.SUCCESS.getStringValue())) {
                throw new xMsgRegistrationException("failed");
            }
        } else {
            throw new xMsgRegistrationException("xMsg actor remove registration timeout");
        }

    }

    /**
     * <p>
     *    Searches registration database (local or global), defined by the
     *    connection socket object, for the publisher or subscriber based
     *    on the xMsg topic components. xMsg topic components, i.e. domain,
     *    subject and types are defined within the xMsgRegistrationData object.
     * </p>
     * @param _connectionSocket connection socket defines the local or
     *                          front-end registration server
     * @param name the name of the sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to find
     *                    publishers, otherwise subscribers
     * @return List of publishers or subscribers that publish/subscribe required topic
     * @throws xMsgDiscoverException
     */
    private List<xMsgRegistrationData> _find(Socket _connectionSocket,
                                             String name,
                                             xMsgRegistrationData data,
                                             boolean isPublisher)
            throws xMsgDiscoverException {

        // Byte array for the registration data serialization
        byte[] dt;

        // Data serialization
        if(data.isInitialized()) {
            dt = data.toByteArray(); // serialize data object
        } else throw new xMsgDiscoverException("some of the data object required fields are not set.");

        // Send topic, sender, followed by the data
        ZMsg msg = new ZMsg();

        // Topic of the message is a string = "findPublisher" or "findSubscriber"
        if(isPublisher) msg.addString(xMsgConstants.FIND_PUBLISHER.getStringValue());
        else msg.addString(xMsgConstants.FIND_SUBSCRIBER.getStringValue());

        // Sender
        msg.addString(name);

        // serialized data
        msg.add(dt);

        // Sending the request
        if (!msg.send(_connectionSocket))throw new xMsgDiscoverException("error sending the data");

        msg.destroy();

        //  Poll socket for a reply, with timeout, make sure server is up and running
        ZMQ.PollItem items[] = {new ZMQ.PollItem(_connectionSocket, ZMQ.Poller.POLLIN)};

        int rc = ZMQ.poll(items, xMsgConstants.FIND_REQUEST_TIMEOUT.getIntValue());

        if (rc != -1 && items[0].isReadable()) {

            // Block until we receive the message
            ZMsg r_msg = ZMsg.recvMsg(_connectionSocket);
            ZFrame r_topic = r_msg.pop();
            ZFrame r_sender = r_msg.pop();

            // de-serialize received message first two components
            String s_topic = new String(r_topic.getData(), ZMQ.CHARSET);
            String s_sender = new String(r_sender.getData(), ZMQ.CHARSET);

            List<xMsgRegistrationData> res = new ArrayList<>();
            while (!r_msg.isEmpty()) {
                ZFrame r_data = r_msg.pop();
                if (r_data == null) throw new xMsgDiscoverException("null xMsg data is received");

                // de-serialize received message components
                final xMsgRegistrationData s_data;
                try {
                    s_data = xMsgRegistrationData.parseFrom(r_data.getData());
                } catch (InvalidProtocolBufferException e) {
                    throw new xMsgDiscoverException(e.getMessage());
                }

                res.add(s_data);
                r_data.destroy();

            }

            // cleanup
            r_topic.destroy();
            r_sender.destroy();
            r_msg.destroy();
            return res;
        } else {
            throw new xMsgDiscoverException("xMsg actor discovery timeout");
        }
    }

    /**
     * <p>
     *     Registers xMsg actor with the front-end registration and discovery server
     * </p>
     * @param name the name of the requester/sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to register
     *                    a publisher, otherwise this is a subscriber registration
     *                    request
     * @throws xMsgRegistrationException
     */
    public void register_fe(String name,
                            xMsgRegistrationData data,
                            boolean isPublisher)
            throws xMsgRegistrationException {
        _register(_feConnection, name, data, isPublisher);
    }

    /**
     * <p>
     *     Registers xMsg actor with the local registration and discovery server
     *     @see #_register(org.zeromq.ZMQ.Socket, String, org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData, boolean)
     * </p>
     * @param name the name of the requester/sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to register
     *                    a publisher, otherwise this is a subscriber registration
     *                    request
     * @throws xMsgRegistrationException
     */
    public void register_local(String name,
                               xMsgRegistrationData data,
                               boolean isPublisher)
            throws xMsgRegistrationException {
        _register(_lnConnection, name, data, isPublisher);
    }

    /**
     * <p>
     *     Removes xMsg actor from the front-end registration and discovery server
     * </p>
     * @param name the name of the requester/sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to remove a
     *                    publisher registration , otherwise remove subscriber
     *                    registration
     * @throws xMsgRegistrationException
     */
    public void removeRegistration_fe(String name,
                                      xMsgRegistrationData data,
                                      boolean isPublisher)
            throws xMsgRegistrationException {
        _remove_registration(_feConnection, name, data, isPublisher);
    }

    /**
     * <p>
     *     Removes xMsg actor from the local registration and discovery server
     * </p>
     * @param name the name of the requester/sender
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to remove a
     *                    publisher registration , otherwise remove subscriber
     *                    registration
     * @throws xMsgRegistrationException
     */
    public void removeRegistration_local(String name,
                                         xMsgRegistrationData data,
                                         boolean isPublisher)
            throws xMsgRegistrationException {
        _remove_registration(_lnConnection, name, data, isPublisher);
    }

    /**
     * <p>
     *     Searches the local registration and discovery databases for an actor
     *     that publishes or subscribes the topic of the interest.
     *     The search criteria i.e. topic is defined within the xMsgRegistrationData object.
     * </p>
     * @param name the name of the sender/requester
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to find
     *                    publishers, otherwise subscribers
     * @return List of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findLocal( String name,
                                                 xMsgRegistrationData data,
                                                 boolean isPublisher)
            throws xMsgDiscoverException {
        return _find(_lnConnection, name, data, isPublisher);
    }

    /**
     * <p>
     *     Searches the front-end registration and discovery databases for an actor
     *     that publishes or subscribes the topic of the interest.
     *     The search criteria i.e. topic is defined within the xMsgRegistrationData object.
     * </p>
     * @param name the name of the sender/requester
     * @param data {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} object
     * @param isPublisher if set to be true then this is a request to find
     *                    publishers, otherwise subscribers
     * @return List of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData} objects
     * @throws xMsgDiscoverException
     */
    public List<xMsgRegistrationData> findGlobal( String name,
                                                  xMsgRegistrationData data,
                                                  boolean isPublisher)
            throws xMsgDiscoverException {
        return _find(_feConnection, name, data, isPublisher);
    }

    /**
     * <p>
     *     Creates and returns zmq socket object
     * </p>
     *
     * @param context zmq context
     * @param socket_type the type of the socket (integer defined by zmq)
     * @param h host name
     * @param port port number
     * @param boc if set 0 socket will be bind, otherwise it will connect.
     *                Note that for xMsg proxies we always connect (boc = 1)
     *                (proxies are XPUB/XSUB sockets).
     * @return zmq socket object
     * @throws org.jlab.coda.xmsg.excp.xMsgException
     */
    public static Socket __zmqSocket(ZContext context,
                                     int socket_type,
                                     String h,
                                     int port,
                                     int boc)
            throws xMsgException {

        // Create a zmq socket
        Socket sb = context.createSocket(socket_type);
        if(sb == null)throw new xMsgException("null zmq socket-base");

        if (boc == xMsgConstants.BIND.getIntValue()) {

            // Bind socket to the host and port
            int bind_port = sb.bind("tcp://" + h + ":" + port);
            if(bind_port <=0){
                throw new xMsgException("can not bind to the port = "+bind_port);
            }
        } else if (boc == xMsgConstants.CONNECT.getIntValue()){

            // Connect to the host and port
            sb.connect("tcp://" + h + ":" + port);
        } else{
            throw new xMsgException("unknown socket bind/connect option");
        }
        return sb;
    }

}
