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

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;
import org.zeromq.ZContext;
import org.zeromq.ZMQException;

public class xMsgConnectionFactory {

    private final xMsgSocketFactory factory;

    public xMsgConnectionFactory(ZContext context) {
        this.factory = new xMsgSocketFactory(context.getContext());
    }

    public xMsgProxyDriver createProxyConnection(xMsgProxyAddress address,
                                                 xMsgConnectionSetup setup) throws xMsgException {

        xMsgProxyDriver connection = new xMsgProxyDriver(address, factory);
        try {
            setup.preConnection(connection.getPubSock());
            setup.preConnection(connection.getSubSock());

            connection.connect();
            if (!connection.checkConnection()) {
                throw new xMsgException("could not connect to " + address);
            }
            setup.postConnection();

            return connection;

        } catch (ZMQException | xMsgException e) {
            connection.close();
            throw e;
        }
    }

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
}
