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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.sys.util.Environment;
import org.zeromq.ZMQ.Socket;

import java.util.Objects;
import java.util.function.Consumer;

public final class xMsgConnectionSetup {

    public static Builder newBuilder() {
        return new Builder();
    }


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

        public Builder withPreConnection(Consumer<Socket> preConnection) {
            Objects.requireNonNull(preConnection, "null pre-connection setup");
            this.preConnection = preConnection;
            return this;
        }

        public Builder withPostConnection(Runnable postConnection) {
            Objects.requireNonNull(postConnection, "null post-connection setup");
            this.postConnection = postConnection;
            return this;
        }

        public Builder withPreSubscription(Consumer<Socket> preSubscription) {
            Objects.requireNonNull(preSubscription, "null pre-subscription setup");
            this.preSubscription = preSubscription;
            return this;
        }

        public Builder withPostSubscription(Runnable postSubscription) {
            Objects.requireNonNull(postSubscription, "null post-subscription setup");
            this.postSubscription = postSubscription;
            return this;
        }

        public Builder withConnectionTimeout(long timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("invalid timeout: " + timeout);
            }
            this.subscriptionTimeout = timeout;
            return this;
        }

        public Builder withSubscriptionTimeout(long timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("invalid timeout: " + timeout);
            }
            this.connectionTimeout = timeout;
            return this;
        }

        public Builder checkConnection(boolean flag) {
            this.checkConnection = flag;
            return this;
        }

        public Builder checkSubscription(boolean flag) {
            this.checkSubscription = flag;
            return this;
        }

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


    // CHECKSTYLE.OFF: ParameterNumber
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
    // CHECKSTYLE.ON: ParameterNumber

    public void preConnection(Socket socket) {
        preConnection.accept(socket);
    }

    public void postConnection() {
        postConnection.run();
    }

    public void preSubscription(Socket socket) {
        preSubscription.accept(socket);
    }

    public void postSubscription() {
        postSubscription.run();
    }

    public long connectionTimeout() {
        return connectionTimeout;
    }

    public long subscriptionTimeout() {
        return subscriptionTimeout;
    }

    public boolean checkConnection() {
        return checkConnection;
    }

    public boolean checkSubscription() {
        return checkSubscription;
    }
}
