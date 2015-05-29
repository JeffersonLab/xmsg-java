/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.coda.xmsg.net;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.excp.xMsgException;

import java.net.SocketException;

import static org.jlab.coda.xmsg.core.xMsgUtil.toHostAddress;

/**
 * xMsg network address.
 * Defines a key constructed as {@code host:port} (xMsg convention) for storing
 * xMsgConnection objects.
 *
 * @author gurjyan
 * @since 1.0
 */
public class xMsgAddress {

    private String host = xMsgConstants.UNDEFINED.getStringValue();
    private int port = xMsgConstants.DEFAULT_PORT.getIntValue();
    private String key = xMsgConstants.UNDEFINED.getStringValue();

    /**
     * Creates an address using the provided host and the default port.
     *
     * @param host the host name
     * @throws SocketException if an I/O error occurs.
     * @throws xMsgException if the host IP address could not be obtained.
     */
    public xMsgAddress(String host) throws SocketException, xMsgException {
        this.host = toHostAddress(host);
        key = this.host + ":" + this.port;
    }

    /**
     * Creates an address using provided host and port.
     *
     * @param host the host name
     * @param port the port number
     * @throws SocketException if an I/O error occurs.
     * @throws xMsgException if the host IP address could not be obtained.
     */
    public xMsgAddress(String host, int port) throws SocketException, xMsgException {
        this.host = toHostAddress(host);
        this.port = port;
        key = this.host + ":" + this.port;
    }

    /**
     * Returns the host name.
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Allows changing the port number.
     * This method should be used with caution, making sure that the
     * xMsgConnection associated with this address is actually created using
     * this new port.
     *
     * @param port the new port number
     */
    public void setPort(int port) {
        this.port = port;
        key = this.host + ":" + this.port;
    }

    /**
     * Returns xMsg address key, constructed as {@code host:port}.
     */
    public String getKey() {
        return key;
    }
}
