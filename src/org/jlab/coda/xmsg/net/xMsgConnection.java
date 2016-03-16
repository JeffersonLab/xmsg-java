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

import org.zeromq.ZMQ.Socket;

/**
 * The standard connection to xMsg nodes.
 * Contains xMsgAddress object and two 0MQ sockets for publishing and
 * subscribing xMsg messages respectfully.
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgConnection {

    private final xMsgProxyAddress address;
    private final String identity;
    private final Socket pubSocket;
    private final Socket subSocket;
    private final Socket ctrlSocket;

    xMsgConnection(xMsgProxyAddress address,
                   String identity,
                   Socket pubSocket,
                   Socket subSocket,
                   Socket ctrSocket) {
        this.address = address;
        this.identity = identity;
        this.pubSocket = pubSocket;
        this.subSocket = subSocket;
        this.ctrlSocket = ctrSocket;
    }

    public xMsgProxyAddress getAddress() {
        return address;
    }

    public String getIdentity() {
        return identity;
    }

    public Socket getPubSock() {
        return pubSocket;
    }

    public Socket getSubSock() {
        return subSocket;
    }

    public Socket getControlSock() {
        return ctrlSocket;
    }
}
