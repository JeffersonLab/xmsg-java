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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.Set;

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
public class xMsgRegDriver {

    /** Front-end registrar server (req/rep) connection socket. */
    private final Socket _feConnection;

    /** Local registrar server (req/rep) connection socket. */
    private final Socket _lnConnection;

    /** zmq context. */
    private final ZContext _context;

    /** Local address. */
    private final String _localAddress;

    /** Front-End address. */
    private final String _frontEndAddress;

    /**
     * Class constructor.
     * Creates sockets to both front-end and local registration and discovery
     * servers. Uses default port.
     *
     * @param localAddress the IP address of the local host
     * @param frontEndAddress the IP address of the front-end host
     */
    public xMsgRegDriver(String localAddress, String frontEndAddress) {
        this(new ZContext(), localAddress, frontEndAddress);
    }

    /**
     * Constructor for testing. Can receive a mock context.
     */
    xMsgRegDriver(ZContext context, String localAddress, String frontEndAddress) {
        _context = context;

        _frontEndAddress = xMsgUtil.validateIP(frontEndAddress);
        _localAddress = xMsgUtil.validateIP(localAddress);

        _feConnection = connect(_frontEndAddress);
        if (!_frontEndAddress.equals(_localAddress)) {
            _lnConnection = connect(_localAddress);
        } else {
            _lnConnection = _feConnection;
        }
    }

    /**
     * Creates and returns 0MQ socket.
     */
    private Socket connect(String host) {
        Socket sb = _context.createSocket(ZMQ.REQ);
        sb.setHWM(0);
        sb.connect("tcp://" + host + ":" + xMsgConstants.REGISTRAR_PORT.toInteger());
        return sb;
    }

    /**
     * Returns the main 0MQ context.
     */
    public ZContext getContext() {
        return _context;
    }

    /**
     * Returns the registered local address.
     */
    public String getLocalAddress() {
        return _localAddress;
    }

    /**
     * Returns the registered front-end address.
     */
    public String getFrontEndAddress() {
        return _frontEndAddress;
    }

    /**
     * Sends a request to the given registrar server and waits the response.
     */
    protected xMsgRegResponse request(Socket socket, xMsgRegRequest request, int timeout)
            throws xMsgRegistrationException {
        ZMsg requestMsg = request.msg();
        try {
            requestMsg.send(socket);
        } catch (ZMQException e) {
            throw new xMsgRegistrationException("error sending the message");
        } finally {
            requestMsg.destroy();
        }

        ZMQ.PollItem[] items = {new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN)};
        int rc = ZMQ.poll(items, timeout);
        if (rc != -1 && items[0].isReadable()) {
            ZMsg responseMsg = ZMsg.recvMsg(socket);
            try {
                xMsgRegResponse response = new xMsgRegResponse(responseMsg);
                String status = response.status();
                if (!status.equals(xMsgConstants.SUCCESS.toString())) {
                    throw new xMsgRegistrationException(status);
                }
                return response;
            } finally {
                responseMsg.destroy();
            }
        } else {
            throw new xMsgRegistrationException("xMsg actor registration timeout");
        }
    }

    /**
     * Checks if the registration data is initialized.
     */
    private void _validateData(xMsgRegistration data) throws xMsgRegistrationException {
        if (!data.isInitialized()) {
            throw new xMsgRegistrationException("The registration data is not complete");
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

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.REGISTER_PUBLISHER.toString() :
                                     xMsgConstants.REGISTER_SUBSCRIBER.toString();
        int timeout = xMsgConstants.REGISTER_REQUEST_TIMEOUT.toInteger();

        xMsgRegRequest request = new xMsgRegRequest(topic, name, data);
        request(socket, request, timeout);
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

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.REMOVE_PUBLISHER.toString() :
                                     xMsgConstants.REMOVE_SUBSCRIBER.toString();
        int timeout = xMsgConstants.REMOVE_REQUEST_TIMEOUT.toInteger();

        xMsgRegRequest request = new xMsgRegRequest(topic, name, data);
        request(socket, request, timeout);
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

        String topic = xMsgConstants.REMOVE_ALL_REGISTRATION.toString();
        int timeout = xMsgConstants.REMOVE_REQUEST_TIMEOUT.toInteger();

        xMsgRegRequest request = new xMsgRegRequest(topic, name, host);
        request(_feConnection, request, timeout);
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
     * @throws xMsgRegistrationException
     */
    private Set<xMsgRegistration> _find(Socket socket,
                                        String name,
                                        xMsgRegistration data,
                                        boolean isPublisher)
            throws xMsgRegistrationException {

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.FIND_PUBLISHER.toString() :
                                     xMsgConstants.FIND_SUBSCRIBER.toString();
        int timeout = xMsgConstants.FIND_REQUEST_TIMEOUT.toInteger();

        xMsgRegRequest request = new xMsgRegRequest(topic, name, data);
        xMsgRegResponse response = request(socket, request, timeout);
        return response.data();
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
     * @throws xMsgRegistrationException
     * @throws xMsgDiscoverException
     */
    public Set<xMsgRegistration> findLocal(String name,
                                           xMsgRegistration data,
                                           boolean isPublisher)
           throws xMsgRegistrationException {
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
    public Set<xMsgRegistration> findGlobal(String name,
                                            xMsgRegistration data,
                                            boolean isPublisher)
            throws xMsgRegistrationException {
        return _find(_feConnection, name, data, isPublisher);
    }
}
