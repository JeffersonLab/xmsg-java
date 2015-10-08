/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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

import java.io.IOException;

import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgAddressException;

/**
 *  xMsg address.
 *
 * @author gurjyan
 * @version 2.x
 */
public abstract class xMsgAddress {

    private final String host;
    private final int port;

    /**
     * Creates an address using provided host and port.
     *
     * @param host the host address
     * @param port the port number
     * @throws xMsgAddressException if the IP address of the host could not be resolved
     */
    public xMsgAddress(String host, int port) {
        try {
            if (host == null) {
                throw new IllegalArgumentException("Null IP address");
            }
            if (port <= 1023) {
                throw new IllegalArgumentException("Null IP address");
            }
            this.host = xMsgUtil.toHostAddress(host);
            this.port = port;
        } catch (IOException e) {
            throw new xMsgAddressException(e);
        }
    }

    /**
     * Returns the host name.
     */
    public String host() {
        return host;
    }

    /**
     * Returns the port number.
     */
    public int port() {
        return port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof xMsgAddress)) {
            return false;
        }

        xMsgAddress that = (xMsgAddress) o;

        if (!host.equals(that.host)) {
            return false;
        }
        if (port != that.port) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }
}
