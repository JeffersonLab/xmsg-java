/*
 *    Copyright (C) 2016. Jefferson Lab (JLAB). All Rights Reserved.
 *    Permission to use, copy, modify, and distribute this software and its
 *    documentation for governmental use, educational, research, and not-for-profit
 *    purposes, without fee and without a signed licensing agreement.
 *
 *    IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 *    INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 *    THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 *    OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *    JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *    THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *    PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 *    HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 *    SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *    This software was developed under the United States Government License.
 *    For more information contact author at gurjyan@jlab.org
 *    Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.coda.xmsg.sys.regdis;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.util.Set;

/**
 * xMsg registration driver.
 *
 * Provides methods for registration and discovery of xMsg actors (i.e.
 * publishers and subscribers) on the specified
 * {@link org.jlab.coda.xmsg.sys.regdis.xMsgRegService xMsg registrar service},
 * using a 0MQ REQ socket.
 *
 * @since 2.x
 */
public class xMsgRegDriver {

    private final xMsgRegAddress address;
    private final xMsgSocketFactory factory;
    private final Socket socket;

    /**
     * Creates a driver to the registrar running in the given address.
     *
     * @param address registrar service address
     * @param factory factory for the ZMQ socket
     * @throws xMsgException
     */
    public xMsgRegDriver(xMsgRegAddress address, xMsgSocketFactory factory) throws xMsgException {
        this.address = address;
        this.factory = factory;
        this.socket = factory.createSocket(ZMQ.REQ);
    }

    /**
     * Connects to the registrar server.
     *
     * @throws xMsgException if the connection failed
     */
    public void connect() throws xMsgException {
        factory.connectSocket(socket, address.host(), address.port());
    }

    /**
     * Sends a request to the registrar server and waits the response.
     *
     * @param request the registration request
     * @param timeout timeout in milli seconds
     *
     * @return the registrar response
     */
    protected xMsgRegResponse request(xMsgRegRequest request, long timeout)
            throws xMsgException {
        ZMsg requestMsg = request.msg();
        try {
            requestMsg.send(socket);
        } catch (ZMQException e) {
            throw new xMsgException("could not send registration request", e);
        }

        try (Poller poller = factory.context().poller(1)) {
            poller.register(socket, ZMQ.Poller.POLLIN);
            poller.poll(timeout);
            if (poller.pollin(0)) {
                xMsgRegResponse response = new xMsgRegResponse(ZMsg.recvMsg(socket));
                String status = response.status();
                if (!status.equals(xMsgRegConstants.SUCCESS)) {
                    throw new xMsgException("registrar server could not process request: "
                                            + status);
                }
                return response;
            } else {
                throw new xMsgException("registrar server response timeout");
            }
        }
    }

    /**
     * Sends a registration request to the registrar service.
     *
     * @param sender the sender of the request
     * @param data the registration data
     * @throws xMsgException
     */
    public void addRegistration(String sender, xMsgRegistration data)
            throws xMsgException {
        addRegistration(sender, data, xMsgConstants.REGISTRATION_TIMEOUT);
    }

    /**
     * Sends a registration request to the registrar service.
     *
     * @param sender the sender of the request
     * @param data the registration data
     * @param timeout the milliseconds to wait for a response
     * @throws xMsgException
     */
    public void addRegistration(String sender, xMsgRegistration data, long timeout)
            throws xMsgException {
        String topic = selectTopic(data.getOwnerType(),
                                   xMsgRegConstants.REGISTER_PUBLISHER,
                                   xMsgRegConstants.REGISTER_SUBSCRIBER);

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        request(request, timeout);
    }

    /**
     * Sends a remove registration request to the registrar service.
     *
     * @param sender the sender of the request
     * @param data the registration data
     * @throws xMsgException
     */
    public void removeRegistration(String sender, xMsgRegistration data)
            throws xMsgException {
        removeRegistration(sender, data, xMsgConstants.REGISTRATION_TIMEOUT);
    }

    /**
     * Sends a remove registration request to the registrar service.
     *
     * @param sender the sender of the request
     * @param data the registration data
     * @param timeout the milliseconds to wait for a response
     * @throws xMsgException
     */
    public void removeRegistration(String sender, xMsgRegistration data, long timeout)
            throws xMsgException {
        String topic = selectTopic(data.getOwnerType(),
                                   xMsgRegConstants.REMOVE_PUBLISHER,
                                   xMsgRegConstants.REMOVE_SUBSCRIBER);

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        request(request, timeout);
    }

    /**
     * Removes registration of all xMsg actors of the specified node.
     * This will remove all publishers and subscribers running
     * on the given host from the registrar service connected
     * by this driver.
     *
     * @param sender the sender of the request
     * @param host the host of the actors to be removed
     * @throws xMsgException
     */
    public void removeAllRegistration(String sender, String host)
            throws xMsgException {
        removeAllRegistration(sender, host, xMsgConstants.REGISTRATION_TIMEOUT);
    }

    /**
     * Removes registration of all xMsg actors of the specified node.
     * This will remove all publishers and subscribers running
     * on the given host from the registrar service connected
     * by this driver.
     *
     * @param sender the sender of the request
     * @param host the host of the actors to be removed
     * @param timeout the milliseconds to wait for a response
     * @throws xMsgException
     */
    public void removeAllRegistration(String sender, String host, long timeout)
            throws xMsgException {
        String topic = xMsgRegConstants.REMOVE_ALL_REGISTRATION;

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, host);
        request(request, timeout);
    }

    /**
     * Sends a request to search the database for publishers or subscribers
     * to a specific topic to the registrar server and waits the response.
     * The topic of interest is defined within the given registration data.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @return set of publishers or subscribers to the required topic.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> findRegistration(String sender, xMsgRegistration data)
            throws xMsgException {
        return findRegistration(sender, data, xMsgConstants.DISCOVERY_TIMEOUT);
    }

    /**
     * Sends a request to search the database for publishers or subscribers
     * to a specific topic to the registrar server and waits the response.
     * The topic of interest is defined within the given registration data.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @param timeout the milliseconds to wait for a response
     * @return set of publishers or subscribers to the required topic.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> findRegistration(String sender,
                                                  xMsgRegistration data,
                                                  long timeout)
            throws xMsgException {
        String topic = selectTopic(data.getOwnerType(),
                                   xMsgRegConstants.FIND_PUBLISHER,
                                   xMsgRegConstants.FIND_SUBSCRIBER);

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        xMsgRegResponse response = request(request, timeout);
        return response.data();
    }

    /**
     * Sends a request to the registrar server to search the database
     * for publishers or subscribers matching specific terms, and waits the response.
     * The search terms should be set in the given registration data. They can be:
     * <ul>
     * <li>domain
     * <li>subject
     * <li>type
     * <li>address
     * </ul>
     * Only defined terms will be used for matching actors.
     * To topic parts should be undefined with {@link xMsgConstants#ANY}.
     * The address should be undefined with {@link xMsgRegConstants#UNDEFINED}.
     * The name is ignored.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @return set of publishers or subscribers that match the given terms.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> filterRegistration(String sender, xMsgRegistration data)
            throws xMsgException {
        return filterRegistration(sender, data, xMsgConstants.DISCOVERY_TIMEOUT);
    }

    /**
     * Sends a request to the registrar server to search the database
     * for publishers or subscribers matching specific terms, and waits the response.
     * The search terms should be set in the given registration data. They can be:
     * <ul>
     * <li>domain
     * <li>subject
     * <li>type
     * <li>address
     * </ul>
     * Only defined terms will be used for matching actors.
     * To topic parts should be undefined with {@link xMsgConstants#ANY}.
     * The address should be undefined with {@link xMsgRegConstants#UNDEFINED}.
     * The name is ignored.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @param timeout the milliseconds to wait for a response
     * @return set of publishers or subscribers that match the given terms.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> filterRegistration(String sender,
                                                    xMsgRegistration data,
                                                    long timeout)
            throws xMsgException {
        String topic = selectTopic(data.getOwnerType(),
                                   xMsgRegConstants.FILTER_PUBLISHER,
                                   xMsgRegConstants.FILTER_SUBSCRIBER);

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        xMsgRegResponse response = request(request, timeout);
        return response.data();
    }

    /**
     * Sends a request to the registrar server to search the database
     * for publishers or subscribers with exactly the same specific topic,
     * and waits the response.
     * <p>
     * This method finds actors with the same topic, instead of doing prefix
     * matching like {@link #findRegistration} or comparing individual topic
     * parts like {@link #filterRegistration}. If the requested topic is {@code A:B},
     * this method will only return actors with topic {@code A:B}, while the
     * filter and find methods will also return actors with matching topics,
     * like {@code A:B:C}.
     * <p>
     * The topic of interest is defined within the given registration data.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @return set of publishers or subscribers that have the exact same topic.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> sameRegistration(String sender,
                                                  xMsgRegistration data)
            throws xMsgException {
        return sameRegistration(sender, data, xMsgConstants.DISCOVERY_TIMEOUT);
    }

    /**
     * Sends a request to the registrar server to search the database
     * for publishers or subscribers with exactly the same specific topic,
     * and waits the response.
     * <p>
     * This method finds actors with the same topic, instead of doing prefix
     * matching like {@link #findRegistration} or comparing individual topic
     * parts like {@link #filterRegistration}. If the requested topic is {@code A:B},
     * this method will only return actors with topic {@code A:B}, while the
     * filter and find methods will also return actors with matching topics,
     * like {@code A:B:C}.
     * <p>
     * The topic of interest is defined within the given registration data.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @param timeout the milliseconds to wait for a response
     * @return set of publishers or subscribers that have the exact same topic.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> sameRegistration(String sender,
                                                  xMsgRegistration data,
                                                  long timeout)
            throws xMsgException {
        String topic = selectTopic(data.getOwnerType(),
                                   xMsgRegConstants.EXACT_PUBLISHER,
                                   xMsgRegConstants.EXACT_SUBSCRIBER);

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        xMsgRegResponse response = request(request, timeout);
        return response.data();
    }

    /**
     * Sends a request to the database to get all publishers or subscribers,
     * and waits the response.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @return set of publishers or subscribers to the required topic.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> allRegistration(String sender, xMsgRegistration data)
            throws xMsgException {
        return allRegistration(sender, data, xMsgConstants.DISCOVERY_TIMEOUT);
    }

    /**
     * Sends a request to the database to get all publishers or subscribers,
     * and waits the response.
     *
     * @param sender the sender of the request
     * @param data the registration data object
     * @param timeout the milliseconds to wait for a response
     * @return set of publishers or subscribers to the required topic.
     * @throws xMsgException
     */
    public Set<xMsgRegistration> allRegistration(String sender,
                                                 xMsgRegistration data,
                                                 long timeout)
            throws xMsgException {
        String topic = selectTopic(data.getOwnerType(),
                                   xMsgRegConstants.ALL_PUBLISHER,
                                   xMsgRegConstants.ALL_SUBSCRIBER);

        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        xMsgRegResponse response = request(request, timeout);
        return response.data();
    }


    /**
     * Closes the connection to the registrar.
     */
    public void close() {
        factory.closeQuietly(socket);
    }

    /**
     * Returns the address of the registrar service.
     */
    public xMsgRegAddress getAddress() {
        return address;
    }


    private static String selectTopic(OwnerType type, String pubTopic, String subTopic) {
        switch (type) {
            case PUBLISHER: return pubTopic;
            case SUBSCRIBER: return subTopic;
            default: throw new RuntimeException("invalid registration owner-type: " + type);
        }
    }
}
