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
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgConnectionSetup;

import java.util.Objects;

/**
 * Setup of an xMsg actor.
 */
public final class xMsgSetup extends ConnectionSetup {

    /**
     * Creates a builder to set options for an xMsg actor.
     *
     * @return a new xMsgSetup builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }


    /**
     * Helps creating the setup for an xMsg actor.
     * All parameters not set will be initialized to their default values.
     */
    public static final class Builder extends ConnectionSetup.Builder<Builder> {

        private xMsgRegAddress registrarAddress = new xMsgRegAddress();
        private xMsgCallbackMode subscriptionMode = xMsgCallbackMode.MULTI_THREAD;
        private int poolSize = xMsgConstants.DEFAULT_POOL_SIZE;

        /**
         * Sets the address of the default registrar.
         * This address will be used by the xMsg registration API when no
         * address is given.
         *
         * @param address the address to the default registrar
         * @return this builder
         */
        public Builder withRegistrar(xMsgRegAddress address) {
            Objects.requireNonNull(address, "null registrar address");
            this.registrarAddress = address;
            return this;
        }

        /**
         * Sets the size of the callback thread-pool.
         *
         * @param poolSize the size of the thread-pool that process the callbacks.
         * @return this builder
         */
        public Builder withPoolSize(int poolSize) {
            if (poolSize <= 0) {
                throw new IllegalArgumentException("invalid pool size: " + poolSize);
            }
            this.poolSize = poolSize;
            return this;
        }

        /**
         * Sets the callback mode for all started subscriptions.
         * This setup will be applied every time a subscription is started.
         *
         * @param mode the callback mode
         * @return this builder
         */
        public Builder withSubscriptionMode(xMsgCallbackMode mode) {
            Objects.requireNonNull(mode, "null subscription mode");
            this.subscriptionMode = mode;
            return this;
        }

        /**
         * Creates the setup for an xMsg actor.
         *
         * @return the actor setup
         */
        public xMsgSetup build() {
            return new xMsgSetup(proxyAddress,
                                 registrarAddress,
                                 subscriptionMode,
                                 conSetup.build(),
                                 poolSize);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }


    private final xMsgRegAddress registrarAddress;
    private final xMsgCallbackMode subscriptionMode;
    private final int poolSize;

    private xMsgSetup(xMsgProxyAddress proxyAddress,
                      xMsgRegAddress registrarAddress,
                      xMsgCallbackMode subscriptionMode,
                      xMsgConnectionSetup connectionSetup,
                      int poolSize) {
        super(proxyAddress, connectionSetup);
        this.registrarAddress = registrarAddress;
        this.subscriptionMode = subscriptionMode;
        this.poolSize = poolSize;
    }

    /**
     * Gets the address to the default registrar.
     *
     * @return the address of the default registrar
     */
    public xMsgRegAddress registrarAddress() {
        return registrarAddress;
    }

    /**
     * Gets the subscription mode to process callbacks.
     *
     * @return the subscription mode
     */
    public xMsgCallbackMode subscriptionMode() {
        return subscriptionMode;
    }

    /**
     * Gets the size of the callback thread-pool.
     *
     * @return the pool size
     */
    public int poolSize() {
        return poolSize;
    }
}
