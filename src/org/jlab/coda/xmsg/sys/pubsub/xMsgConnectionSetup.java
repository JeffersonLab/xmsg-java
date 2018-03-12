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

package org.jlab.coda.xmsg.sys.pubsub;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.sys.util.Environment;
import org.zeromq.ZMQ.Socket;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Setup of an xMsg connection.
 */
public final class xMsgConnectionSetup {

    /**
     * Creates a new setup builder.
     *
     * @return the builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }


    /**
     * Helps creating the setup for the xMsg connection(s).
     */
    public static final class Builder {

        private Consumer<Socket> preConnection;
        private Runnable postConnection;

        private Consumer<Socket> preSubscription;
        private Runnable postSubscription;

        private long connectionTimeout;
        private long subscriptionTimeout;

        private boolean checkConnection;
        private boolean checkSubscription;

        private Builder() {
            final long postConSleep = Environment.getLong("XMSG_POST_CONNECTION_SLEEP", 0);
            final long postSubSleep = Environment.getLong("XMSG_POST_SUBSCRIPTION_SLEEP", 10);

            preConnection = (s) -> { };
            postConnection = () -> xMsgUtil.sleep(postConSleep);

            preSubscription = (s) -> { };
            postSubscription = () -> xMsgUtil.sleep(postSubSleep);

            connectionTimeout = Environment.getLong("XMSG_CONNECTION_TIMEOUT",
                                                    xMsgConstants.CONNECTION_TIMEOUT);
            subscriptionTimeout = Environment.getLong("XMSG_SUBSCRIPTION_TIMEOUT",
                                                      xMsgConstants.SUBSCRIPTION_TIMEOUT);

            checkConnection = !Environment.isDefined("XMSG_NO_CHECK_CONNECTION");
            checkSubscription = !Environment.isDefined("XMSG_NO_CHECK_SUBSCRIPTION");
        }

        /**
         * Sets the action to run before connecting the socket.
         *
         * @param preConnection a consumer that acts on the socket
         *        before it is connected
         * @return this builder
         */
        public Builder withPreConnection(Consumer<Socket> preConnection) {
            Objects.requireNonNull(preConnection, "null pre-connection setup");
            this.preConnection = preConnection;
            return this;
        }

        /**
         * Sets the action to run after connecting the socket.
         *
         * @param postConnection a runnable that runs after the socket was
         *                       connected
         * @return this builder
         */
        public Builder withPostConnection(Runnable postConnection) {
            Objects.requireNonNull(postConnection, "null post-connection setup");
            this.postConnection = postConnection;
            return this;
        }


        /**
         * Sets the action to run before subscribing the socket.
         *
         * @param preSubscription a consumer that acts on the socket
         *        before it is subscribed
         * @return this builder
         */
        public Builder withPreSubscription(Consumer<Socket> preSubscription) {
            Objects.requireNonNull(preSubscription, "null pre-subscription setup");
            this.preSubscription = preSubscription;
            return this;
        }

        /**
         * Sets the action to run after subscribing the socket.
         *
         * @param postSubscription a runnable that runs after the socket was
         *                         subscribed
         * @return this builder
         */
        public Builder withPostSubscription(Runnable postSubscription) {
            Objects.requireNonNull(postSubscription, "null post-subscription setup");
            this.postSubscription = postSubscription;
            return this;
        }

        /**
         * Sets a timeout to wait for a confirmation than the socket was
         * connected.
         *
         * @param timeout the time to wait, in milliseconds
         * @return this builder
         */
        public Builder withConnectionTimeout(long timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("invalid timeout: " + timeout);
            }
            this.subscriptionTimeout = timeout;
            return this;
        }

        /**
         * Sets a timeout to wait for a confirmation than the socket was
         * subscribed.
         *
         * @param timeout the time to wait, in milliseconds
         * @return this builder
         */
        public Builder withSubscriptionTimeout(long timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("invalid timeout: " + timeout);
            }
            this.connectionTimeout = timeout;
            return this;
        }

        /**
         * Sets if the connection must be validated with control messages.
         *
         * @param flag if true, the connection will be checked
         * @return this builder
         */
        public Builder checkConnection(boolean flag) {
            this.checkConnection = flag;
            return this;
        }

        /**
         * Sets if the subscription must be validated with control messages.
         *
         * @param flag if true, the connection will be checked
         * @return this builder
         */
        public Builder checkSubscription(boolean flag) {
            this.checkSubscription = flag;
            return this;
        }

        /**
         * Creates the setup.
         *
         * @return the created connection setup
         */
        public xMsgConnectionSetup build() {
            return new xMsgConnectionSetup(preConnection,
                                           postConnection,
                                           preSubscription,
                                           postSubscription,
                                           connectionTimeout,
                                           subscriptionTimeout,
                                           checkConnection,
                                           checkSubscription);
        }
    }


    private final Consumer<Socket> preConnection;
    private final Runnable postConnection;

    private final Consumer<Socket> preSubscription;
    private final Runnable postSubscription;

    private final long connectionTimeout;
    private final long subscriptionTimeout;

    private final boolean checkConnection;
    private final boolean checkSubscription;


    // checkstyle.off: ParameterNumber
    private xMsgConnectionSetup(Consumer<Socket> preConnection,
                                Runnable postConnection,
                                Consumer<Socket> preSubscription,
                                Runnable postSubscription,
                                long connectionTimeout,
                                long subscriptionTimeout,
                                boolean checkConnection,
                                boolean checkSubscription) {
        this.preConnection = preConnection;
        this.postConnection = postConnection;
        this.preSubscription = preSubscription;
        this.postSubscription = postSubscription;
        this.connectionTimeout = connectionTimeout;
        this.subscriptionTimeout = subscriptionTimeout;
        this.checkConnection = checkConnection;
        this.checkSubscription = checkSubscription;
    }
    // checkstyle.on: ParameterNumber

    /**
     * Runs the pre-connection action on the given socket.
     *
     * @param socket the socket to be consumed
     */
    public void preConnection(Socket socket) {
        preConnection.accept(socket);
    }

    /**
     * Runs the post-connection action.
     */
    public void postConnection() {
        postConnection.run();
    }

    /**
     * Runs the pre-subscription action on the given socket.
     *
     * @param socket the socket to be consumed
     */
    public void preSubscription(Socket socket) {
        preSubscription.accept(socket);
    }

    /**
     * Runs the post-subscription action.
     */
    public void postSubscription() {
        postSubscription.run();
    }

    /**
     * Gets the timeout for checking the connection.
     *
     * @return the timeout to wait for a control message confirming the
     *         connection.
     */
    public long connectionTimeout() {
        return connectionTimeout;
    }


    /**
     * Gets the timeout for checking the subscription.
     *
     * @return the timeout to wait for a control message confirming the
     *         connection.
     */
    public long subscriptionTimeout() {
        return subscriptionTimeout;
    }

    /**
     * Gets if the connection must be checked.
     *
     * @return true if the connection must be validated with control messages.
     */
    public boolean checkConnection() {
        return checkConnection;
    }

    /**
     * Gets if the subscription must be checked.
     *
     * @return true if the subscription must be validated with control messages.
     */
    public boolean checkSubscription() {
        return checkSubscription;
    }
}
