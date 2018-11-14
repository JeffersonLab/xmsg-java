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

package org.jlab.coda.xmsg.net;

import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.sys.pubsub.xMsgConnectionSetup;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;
import org.zeromq.ZMQException;

/**
 * Creates new xMsg connections.
 */
public class xMsgConnectionFactory {

    private final xMsgContext context;
    private final xMsgSocketFactory factory;

    /**
     * Creates a new connection factory.
     *
     * @param context the context to handle connections
     */
    public xMsgConnectionFactory(xMsgContext context) {
        this.context = context;
        this.factory = new xMsgSocketFactory(context.getContext());
    }

    /**
     * Creates a new subscriber connection.
     *
     * @param address the address of the proxy used by the connection
     * @param setup the settings of the connection
     * @return a new connection that can subscribe to topics
     *         through the proxy running on the given address
     * @throws xMsgException if the connection could not be created
     */
    public xMsgProxyDriver createSubscriberConnection(xMsgProxyAddress address,
                                                      xMsgConnectionSetup setup)
            throws xMsgException {
        xMsgProxyDriver connection = xMsgProxyDriver.subscriber(address, factory);
        prepareProxyConnection(connection, setup);
        return connection;
    }

    /**
     * Creates a new publisher connection.
     *
     * @param address the address of the proxy used by the connection
     * @param setup the settings of the connection
     * @return a new connection that can publish to topics
     *         through the proxy running on the given address
     * @throws xMsgException if the connection could not be created
     */
    public xMsgProxyDriver createPublisherConnection(xMsgProxyAddress address,
                                                     xMsgConnectionSetup setup)
            throws xMsgException {
        xMsgProxyDriver connection = xMsgProxyDriver.publisher(address, factory);
        prepareProxyConnection(connection, setup);
        return connection;
    }

    private void prepareProxyConnection(xMsgProxyDriver connection, xMsgConnectionSetup setup)
            throws xMsgException {
        try {
            setup.preConnection(connection.getSocket());
            connection.connect();
            xMsgUtil.sleep(10);
            if (setup.checkConnection() && !connection.checkConnection(setup.connectionTimeout())) {
                throw new xMsgException("could not connect to " + connection.getAddress());
            }
            setup.postConnection();
        } catch (ZMQException | xMsgException e) {
            connection.close();
            throw e;
        }
    }

    /**
     * Creates a new registrar connection.
     *
     * @param address the address of the registrar used by the connection
     * @return a new connection that can communicate with
     *         the registrar running on the given address
     * @throws xMsgException if the connection could not be created
     */
    public xMsgRegDriver createRegistrarConnection(xMsgRegAddress address) throws xMsgException {
        xMsgRegDriver driver = new xMsgRegDriver(address, factory);
        try {
            driver.connect();
            return driver;
        } catch (ZMQException | xMsgException e) {
            driver.close();
            throw e;
        }
    }

    /**
     * Returns the context used by this factory.
     *
     * @return the context
     */
    public xMsgContext getContext() {
        return context;
    }
}
