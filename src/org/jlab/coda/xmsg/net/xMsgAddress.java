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

import static org.jlab.coda.xmsg.core.xMsgUtil.host_to_ip;

/**
 *<p>
 *    xMsg network address container class.
 *    Defines a key constructed as host:port (xMsg
 *    convention) for storing xMsgConnection objects.
 *</p>
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgAddress {

    private String host = xMsgConstants.UNDEFINED.getStringValue();
    private int port = xMsgConstants.DEFAULT_PORT.getIntValue();
    private String key = xMsgConstants.UNDEFINED.getStringValue();

    public xMsgAddress(String host, boolean n) {
        this.host = host;
        key = this.host + ":" + this.port;

    }
    /**
     * <p>
     *     Constructor that converts host name into a
     *     dotted notation of the IP address.
     *     This constructor uses xMsg default port
     * </p>
     * @param host name
     * @throws xMsgException
     */
    public xMsgAddress(String host) throws xMsgException, SocketException {
        this.host = host_to_ip(host);
        key = this.host+":"+this.port;
    }

    /**
     * <p>
     *     Constructor that creates an instance of the
     *     cMsgAddress using user provided host and port
     * </p>
     * @param host name
     * @param port port number
     * @throws xMsgException
     */
    public xMsgAddress(String host, int port) throws xMsgException, SocketException {
        this.host = host_to_ip(host);
        this.port = port;
        key = this.host+":"+this.port;
    }

    /**
     * Returns the host name
     * @return hostname˜

     */
    public String getHost() {
        return host;
    }


    /**
     * Returns the port number
     * @return port number
     */
    public int getPort() {
        return port;
    }

    /**
     * <p>
     *     Allows to change the port number.
     *     This method should be used with caution, making sure
     *     that the xMsgConnection associated with this address
     *     is actually created using this new port.
     * </p>
     * @param port port number
     */
    public void setPort(int port) {
        this.port = port;
        key = this.host+":"+this.port;
    }

    /**
     * Returns xMsg address key, constructed asd host:ort
     * @return address key
     */
    public String getKey() {
        return key;
    }
}
