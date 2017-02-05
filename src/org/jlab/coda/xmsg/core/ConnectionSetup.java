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

import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgConnectionSetup;
import org.zeromq.ZMQ.Socket;

import java.util.Objects;
import java.util.function.Consumer;

class ConnectionSetup {

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

        /**
         * Sets the user-setup function to run before connecting with a proxy.
         * This function can be used to configure the socket before it is
         * connected. It will be called for both pub/sub sockets. It should be
         * used to set options on the socket.
         *
         * @param setup the pre-connection setup function
         * @return this builder
         * @see <a href="http://api.zeromq.org/3-2:zmq-setsockopt">zmq-setsockopt</a>
         */
        public T withPreConnectionSetup(Consumer<Socket> setup) {
            this.conSetup.withPreConnection(setup);
            return getThis();
        }

        /**
         * Sets the user-setup function to run after connecting with a proxy.
         * This function can be used to run some action after the socket has
         * been connected. For example, sleep a while to give time to the
         * sockets to be actually connected internally.
         *
         * @param setup the post-connection setup function
         * @return this builder
         */
        public T withPostConnectionSetup(Runnable setup) {
            this.conSetup.withPostConnection(setup);
            return getThis();
        }

        /**
         * Sets the user-setup function to run before subscribing with a proxy.
         * This function can be used to configure a SUB socket before it is
         * subscribed.
         *
         * @param setup the pre-subscription setup function
         * @return this builder
         */
        public T withPreSubscriptionSetup(Consumer<Socket> setup) {
            this.conSetup.withPreSubscription(setup);
            return getThis();
        }

        /**
         * Sets the user-setup function to run after subscribing with a proxy.
         * This function can be used to run some action after a SUB socket has
         * been subscribed. For example, sleep a while to give time to the
         * subscription to propagate to the publishers.
         *
         * @param setup the post-subscription setup function
         * @return this builder
         */
        public T withPostSubscriptionSetup(Runnable setup) {
            this.conSetup.withPostSubscription(setup);
            return getThis();
        }

        /**
         * Sets the timeout for checking a new connection.
         *
         * @param timeout the check connection timeout
         * @return this builder
         */
        public T withConnectionTimeout(long timeout) {
            this.conSetup.withConnectionTimeout(timeout);
            return getThis();
        }

        /**
         * Sets the timeout for checking a new subscription.
         *
         * @param timeout the check subscription timeout
         * @return this builder
         */
        public T withSubscriptionTimeout(long timeout) {
            this.conSetup.withSubscriptionTimeout(timeout);
            return getThis();
        }

        /**
         * Sets if created connections must be checked by sending control
         * messages to the proxy.
         *
         * @param flag true to use control messages to check a new connection
         * @return this builder
         */
        public T checkConnection(boolean flag) {
            this.conSetup.checkConnection(flag);
            return getThis();
        }

        /**
         * Sets if started subscriptions must be checked by sending control
         * messages to the proxy.
         *
         * @param flag true to use control messages to check a new subscription
         * @return this builder
         */
        public T checkSubscription(boolean flag) {
            this.conSetup.checkSubscription(flag);
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
