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
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.util.Set;

/**
 * <p>
 *     xMsg registration driver. Provides methods for registration and
 *     discovery of xMsg actors, i.e. publishers and subscribers.
 *     Creates 0MQ socket connection to the xMsg registrar service
 *     {@link org.jlab.coda.xmsg.xsys.regdis.xMsgRegService}
 * </p>
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgRegDriver {

    /** 0MQ context.*/
    private final ZContext _context;
    /** Registrar service host IP.*/
    private final String _registrarIp;
    /**
     * Registrar service server (req/rep) connection socket.
     */
    private Socket _connectionSocket = null;
    /** Registrar service listening port.*/
    private int _registrarPort = xMsgConstants.REGISTRAR_PORT.getIntValue();

    /** Registrar service connection address */
    private String _address;


    /**
     * <p>
     *     Constructor
     * </p>
     * @param context 0MQ context
     * @param ip registrar service IP address
     * @param port registrar service listening port
     */
    public xMsgRegDriver(ZContext context, String ip, int port) {
        _context = context;
        _registrarIp = xMsgUtil.validateIP(ip);
        _registrarPort = port;
        _address = "tcp://" + _registrarIp + ":" + _registrarPort;
    }

    /**
     * <p>
     * Constructor. Uses xMsg registrar service default port
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#REGISTRAR_PORT}
     * </p>
     *
     * @param context 0MQ context
     * @param ip      registrar service IP address
     */
    public xMsgRegDriver(ZContext context, String ip) {
        _context = context;
        _registrarIp = xMsgUtil.validateIP(ip);
        _address = "tcp://" + _registrarIp + ":" + _registrarPort;
    }

    /**
     * <p>
     * Defines if the 0MQ socket has been
     * created to the registrar services.
     * </p>
     *
     * @return true if connection socket is made
     */
    public boolean isConnected() {
        return _connectionSocket!=null;
    }

    /**
     * <p>
     *     Creates and returns 0MQ socket to the Registrar service
     * </p>
     *
     */
    public void connect() {
        _connectionSocket = _context.createSocket(ZMQ.REQ);
        _connectionSocket.setHWM(0);
        _connectionSocket.connect(_address);
    }

    /**
     * <p>
     *     Disconnects from the registrar
     *     service and closes 0MQ socket.
     * </p>
     */
    public void disconnect() {
        _connectionSocket.disconnect(_address);
        _connectionSocket.close();
    }

    /**
     * <p>
     *     Utility method that returns 0MQ context that
     *     was used to create this xMsg registration driver.
     * </p>
     * @return 0MQ context {@link org.zeromq.ZContext}
     */
    public ZContext getContext() {
        return _context;
    }

    /**
     * <p>
     *     Utility method that returns IP address of the
     *     registrar service that was used to create this
     *     xMsg registration driver.
     * </p>
     * @return IP address of the registrar service
     */
    public String getIp() {
        return _registrarIp;
    }

    /**
     * <p>
     * Utility method that returns port of the
     * registrar service that was used to create this
     * xMsg registration driver.
     * </p>
     *
     * @return port of the registrar service
     */
    public int getPort() {
        return _registrarPort;
    }

    /**
     * <p>
     * Utility method that returns server address of the
     * registrar service that was used to create this
     * xMsg registration driver.
     * </p>
     *
     * @return server address the registrar service
     */
    public String getAddress() {
        return _address;
    }

    /**
     * <p>
     *     Sends sync request to the registrar service and
     *     receives the xMsg response object
     *     {@link org.jlab.coda.xmsg.xsys.regdis.xMsgRegResponse}
     * </p>
     *
     * @param socket 0MQ socket to the registrar service
     * @param request xMsg request object
     *                {@link org.jlab.coda.xmsg.xsys.regdis.xMsgRegRequest}
     *
     * @param timeout timeout in milli seconds
     *
     * @return xMsg response object
     *         {@link org.jlab.coda.xmsg.xsys.regdis.xMsgRegResponse}
     *
     * @throws xMsgRegistrationException
     */
    protected xMsgRegResponse request(Socket socket, xMsgRegRequest request, int timeout)
            throws xMsgRegistrationException {
        ZMsg requestMsg = request.msg();
        try {
            requestMsg.send(socket);
        } catch (ZMQException e) {
            throw new xMsgRegistrationException("xMsgRegDriver: Error sending the message");
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
                if (!status.equals(xMsgConstants.SUCCESS.getStringValue())) {
                    throw new xMsgRegistrationException(status);
                }
                return response;
            } finally {
                responseMsg.destroy();
            }
        } else {
            throw new xMsgRegistrationException("xMsgRegDriver: Actor registration timeout");
        }
    }

    /**
     * <p>
     *     Checks the validity of the registration data
     * </p>
     * @param data registration data
     *             {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration}
     * @throws xMsgRegistrationException
     */
    private void _validateData(xMsgRegistration data) throws xMsgRegistrationException {
        if (!data.isInitialized()) {
            throw new xMsgRegistrationException("xMsgRegDriver: The registration data is incomplete");
        }
    }

    /**
     * <p>
     *     Sends a registration request to the registrar service,
     *     defined at the constructor. Request is wired using xMsg
     *     message construct, that have 3 part: topic, sender, and data.
     * </p>
     *
     * @param data the registration data object
     *             {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration}
     * @param isPublisher if true then this is a request to register a publisher,
     *                     otherwise this is a request to register a subscriber
     * @throws xMsgRegistrationException
     */
    public void register(xMsgRegistration data,
                         boolean isPublisher)
            throws xMsgRegistrationException {

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.REGISTER_PUBLISHER.getStringValue() :
                xMsgConstants.REGISTER_SUBSCRIBER.getStringValue();
        int timeout = xMsgConstants.REGISTER_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, data.getName(), data);
        request(_connectionSocket, request, timeout);
    }

    /**
     * <p>
     *     Sends a remove registration request to the registrar service,
     *     defined at the constructor. Request is wired using xMsg
     *     message construct, that have 3 part: topic, sender, and data.
     * </p>
     *
     * @param data the registration data object
     *             {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration}
     * @param isPublisher if true then this is a request to register a publisher,
     *                     otherwise this is a request to register a subscriber
     * @throws xMsgRegistrationException
     */
    public void removeRegistration(xMsgRegistration data,
                                   boolean isPublisher)
            throws xMsgRegistrationException {

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.REMOVE_PUBLISHER.getStringValue() :
                xMsgConstants.REMOVE_SUBSCRIBER.getStringValue();
        int timeout = xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, data.getName(), data);
        request(_connectionSocket, request, timeout);
    }

    /**
     * <p>
     *     Removes registration of all xMsg actors of the registrar service,
     *     defined at the constructor. This will remove all publishers and
     *     subscribers from the registrar service registration database.
     *     This method is usually called by the xMsgNode registrar when
     *     it shuts down or gets interrupted.
     * </p>
     *
     * @throws xMsgRegistrationException
     */
    public void removeAll()
            throws xMsgRegistrationException {

        String topic = xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue();
        int timeout = xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, "anonymous", _registrarIp);
        request(_connectionSocket, request, timeout);
    }

    /**
     * <p>
     *     Searched the registration database of the registrar service,
     *     defined bu the constructor. This will search the database for
     *     publishers aor subscribers to a specific topic. The topic of
     *     an interest is defined within the xMsgRegistration data object
     *     {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration}
     * </p>
     *
     * @param data the registration data object
     * @param isPublisher if true then this is a request to find publishers,
     *                     otherwise this is a request to find subscribers
     * @return set of publishers or subscribers to the required topic.
     * @throws xMsgRegistrationException
     */
    public Set<xMsgRegistration> findRegistration(xMsgRegistration data,
                                                  boolean isPublisher)
            throws xMsgRegistrationException {

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.FIND_PUBLISHER.getStringValue() :
                xMsgConstants.FIND_SUBSCRIBER.getStringValue();
        int timeout = xMsgConstants.FIND_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, data.getName(), data);
        xMsgRegResponse response = request(_connectionSocket, request, timeout);
        return response.data();
    }

}
