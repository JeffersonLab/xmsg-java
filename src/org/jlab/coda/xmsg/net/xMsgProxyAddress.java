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

import java.io.UncheckedIOException;

/**
 * xMsg proxy address.
 *
 * @version 2.x
 */
public class xMsgProxyAddress {

    private final String host;
    private final int pubPort;
    private final int subPort;

    /**
     * Creates an address with default host and ports.
     *
     * @throws UncheckedIOException if the IP address of the host could not be resolved
     */
    public xMsgProxyAddress() {
        this("localhost", xMsgConstants.DEFAULT_PORT);
    }

    /**
     * Creates an address with provided host and default ports.
     *
     * @param host the host IP address
     * @throws UncheckedIOException if the IP address of the host could not be resolved
     */
    public xMsgProxyAddress(String host) {
        this(host, xMsgConstants.DEFAULT_PORT);
    }

    /**
     * Creates an address using provided host and publication port.
     * Subscription port is the publication port plus one.
     *
     * @param host the host address
     * @param port the publication port number
     * @throws UncheckedIOException if the IP address of the host could not be resolved
     */
    public xMsgProxyAddress(String host, int port) {
        if (host == null) {
            throw new IllegalArgumentException("null IP address");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("invalid port: " + port);
        }
        this.host = xMsgUtil.toHostAddress(host);
        this.pubPort = port;
        this.subPort = port + 1;
    }

    /**
     * Returns the host address.
     *
     * @return the host IP address
     */
    public String host() {
        return host;
    }

    /**
     * Returns the publication port number.
     *
     * @return the PUB port
     */
    public int pubPort() {
        return pubPort;
    }

    /**
     * Returns the subscription port number.
     *
     * @return the SUB port
     */
    public int subPort() {
        return subPort;
    }

    @Override
    public String toString() {
        return host + ":" + pubPort;
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + pubPort;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        xMsgProxyAddress other = (xMsgProxyAddress) obj;
        return host.equals(other.host) && pubPort == other.pubPort;
    }
}
