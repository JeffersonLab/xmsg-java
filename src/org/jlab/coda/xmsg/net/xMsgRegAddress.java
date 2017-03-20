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
 * xMsg Registrar address.
 *
 * @version 2.x
 */
public class xMsgRegAddress {

    private final String host;
    private final int port;

    /**
     * Creates an address with default host and port.
     *
     * @throws UncheckedIOException if the IP address of the host could not be resolved
     */
    public xMsgRegAddress() {
        this("localhost", xMsgConstants.REGISTRAR_PORT);
    }

    /**
     * Creates an address with provided host and default port.
     *
     * @param host the host IP address
     * @throws UncheckedIOException if the IP address of the host could not be resolved
     */
    public xMsgRegAddress(String host) {
        this(host, xMsgConstants.REGISTRAR_PORT);
    }

    /**
     * Creates an address using provided host and port.
     *
     * @param host the host address
     * @param port the port number
     * @throws UncheckedIOException if the IP address of the host could not be resolved
     */
    public xMsgRegAddress(String host, int port) {
        if (host == null) {
            throw new IllegalArgumentException("null IP address");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("invalid port: " + port);
        }
        this.host = xMsgUtil.toHostAddress(host);
        this.port = port;
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
     * Returns the port number.
     *
     * @return the port
     */
    public int port() {
        return port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + port;
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
        xMsgRegAddress other = (xMsgRegAddress) obj;
        return host.equals(other.host) && port == other.port;
    }
}
