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
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.util.Set;

/**
 * xMsg registration driver.
 *
 * Provides methods for registration and discovery of xMsg actors (i.e.
 * publishers and subscribers) on the specified
 * {@link org.jlab.coda.xmsg.xsys.regdis.xMsgRegService xMsg registrar service},
 * using a 0MQ REQ socket.
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgRegDriver {

    // 0MQ context.
    private final ZContext _context;

    // Registrar address
    private final xMsgRegAddress _address;

    // Registrar server (req/rep) connection socket.
    private Socket _connectionSocket = null;


    /**
     * Creates a driver to the registrar running in the given address.
     *
     * @param context 0MQ context
     * @param address registrar service address
     */
    public xMsgRegDriver(ZContext context, xMsgRegAddress address) {
        _context = context;
        _address = address;
        String addr = "tcp://" + address.host() + ":" + address.port();
        _connectionSocket = _context.createSocket(ZMQ.REQ);
        _connectionSocket.setHWM(0);
        _connectionSocket.connect(addr);
    }

    /**
     * Closes the connection to the registrar.
     * This driver should not be used after calling this method.
     */
    public void destroy() {
        _context.destroySocket(_connectionSocket);
    }

    /**
     * Returns the address of the registrar service.
     */
    public xMsgRegAddress getAddress() {
        return _address;
    }

    /**
     * Sends a request to the registrar server and waits the response.
     *
     * @param request the registration request
     * @param timeout timeout in milli seconds
     *
     * @return the registrar response
     */
    protected xMsgRegResponse request(xMsgRegRequest request, int timeout)
            throws xMsgException {
        ZMsg requestMsg = request.msg();
        try {
            requestMsg.send(_connectionSocket);
        } catch (ZMQException e) {
            throw new xMsgException("xMsg-Error: sending registration message. " +
                    e.getMessage(), e.getCause());
        } finally {
            requestMsg.destroy();
        }

        ZMQ.PollItem[] items = {new ZMQ.PollItem(_connectionSocket, ZMQ.Poller.POLLIN)};
        int rc = ZMQ.poll(items, timeout);
        if (rc != -1 && items[0].isReadable()) {
            ZMsg responseMsg = ZMsg.recvMsg(_connectionSocket);
            try {
                xMsgRegResponse response = new xMsgRegResponse(responseMsg);
                String status = response.status();
                if (!status.equals(xMsgConstants.SUCCESS.getStringValue())) {
                    throw new xMsgException("xMsg-Error: unsuccessful registration: " + status);
                }
                return response;
            } finally {
                responseMsg.destroy();
            }
        } else {
            throw new xMsgException("xMsg-Error: Actor registration timeout");
        }
    }

    /**
     * Checks if the registration data is initialized.
     */
    private void _validateData(xMsgRegistration data) throws xMsgException {
        if (!data.isInitialized()) {
            throw new xMsgException("xMsg-Error: registration data is incomplete");
        }
    }

    /**
     * Sends a registration request to the registrar service.
     *
     * @param data the registration data
     * @param isPublisher if true then this is a request to register a publisher,
     *                     otherwise this is a request to register a subscriber
     * @throws xMsgException
     */
    public void register(xMsgRegistration data,
                         boolean isPublisher)
            throws xMsgException {

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.REGISTER_PUBLISHER.getStringValue() :
                                     xMsgConstants.REGISTER_SUBSCRIBER.getStringValue();
        int timeout = xMsgConstants.REGISTER_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, data.getName(), data);
        request(request, timeout);
    }

    /**
     * Sends a remove registration request to the registrar service.
     *
     * @param data the registration data
     * @param isPublisher if true then this is a request to register a publisher,
     *                     otherwise this is a request to register a subscriber
     * @throws xMsgException
     */
    public void removeRegistration(xMsgRegistration data,
                                   boolean isPublisher)
            throws xMsgException {

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.REMOVE_PUBLISHER.getStringValue() :
                                     xMsgConstants.REMOVE_SUBSCRIBER.getStringValue();
        int timeout = xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, data.getName(), data);
        request(request, timeout);
    }

    /**
     * Removes registration of all xMsg actors of the specified node.
     * This will remove all publishers and subscribers running
     * on the given host from the registrar service connected
     * by this driver.
     * <p>
     * This method is usually called by the xMsgNode registrar when
     * it shuts down or gets interrupted.
     *
     * @throws xMsgException
     */
    public void removeAllRegistration(String sender, String host)
            throws xMsgException {

        String topic = xMsgConstants.REMOVE_ALL_REGISTRATION.getStringValue();
        int timeout = xMsgConstants.REMOVE_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, host);
        request(request, timeout);
    }

    /**
     * Searches the connected registrar database for the given topic.
     * defined by the constructor. This will search the database for
     * publishers or subscribers to a specific topic. The topic of
     * interest is defined within the given registration data.
     *
     * @param data the registration data object
     * @param isPublisher if true then this is a request to find publishers,
     *                     otherwise this is a request to find subscribers
     * @return set of publishers or subscribers to the required topic.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> findRegistration(xMsgRegistration data,
                                                  boolean isPublisher)
            throws xMsgException {

        _validateData(data);

        String topic = isPublisher ? xMsgConstants.FIND_PUBLISHER.getStringValue() :
                                     xMsgConstants.FIND_SUBSCRIBER.getStringValue();
        int timeout = xMsgConstants.FIND_REQUEST_TIMEOUT.getIntValue();

        xMsgRegRequest request = new xMsgRegRequest(topic, data.getName(), data);
        xMsgRegResponse response = request(request, timeout);
        return response.data();
    }
}
