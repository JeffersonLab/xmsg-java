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

package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.net.xMsgConnectionSetup;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

import java.util.Objects;

abstract class ConnectionSetup {

    abstract static class Builder<T extends Builder<T>> {

        protected xMsgProxyAddress proxyAddress = new xMsgProxyAddress();
        protected xMsgConnectionSetup.Builder conSetup = xMsgConnectionSetup.newBuilder();

        /**
         * Sets the address of the default proxy.
         * This address will be used by the xMsg pub-sub API when no address is
         * given.
         *
         * @param address the address to the default proxy
         * @return this builder
         */
        public T withProxy(xMsgProxyAddress address) {
            Objects.requireNonNull(address, "null proxy address");
            this.proxyAddress = address;
            return getThis();
        }

        abstract T getThis();
    }


    private final xMsgProxyAddress proxyAddress;
    private final xMsgConnectionSetup connectionSetup;

    protected ConnectionSetup(xMsgProxyAddress proxyAddress,
                              xMsgConnectionSetup connectionSetup) {
        this.proxyAddress = proxyAddress;
        this.connectionSetup = connectionSetup;
    }

    /**
     * Gets the address to the default proxy.
     *
     * @return the address of the default proxy
     */
    public xMsgProxyAddress proxyAddress() {
        return proxyAddress;
    }

    /**
     * Gets the setup for new connections.
     *
     * @return the connection setup
     */
    xMsgConnectionSetup connectionSetup() {
        return connectionSetup;
    }
}
