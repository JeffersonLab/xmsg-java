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

import org.zeromq.ZMQ.Socket;

/**
 * The standard connection to xMsg nodes.
 * Contains xMSgAddress object and two 0MQ sockets for publishing and
 * subscribing xMsg messages respectfully.
 *
 * @author gurjyan
 * @since 2.x
 */
public class xMsgConnection {

    private xMsgProxyAddress address;
    private Socket pubSock = null;
    private Socket subSock = null;
    private Socket ctrlSock = null;
    private String identity = null;


    public xMsgProxyAddress getAddress() {
        return address;
    }

    public void setAddress(xMsgProxyAddress address) {
        this.address = address;
    }

    public Socket getPubSock() {
        return pubSock;
    }

    public void setPubSock(Socket pubSock) {
        this.pubSock = pubSock;
    }

    public Socket getSubSock() {
        return subSock;
    }

    public void setSubSock(Socket subSock) {
        this.subSock = subSock;
    }

    public Socket getControlSock() {
        return ctrlSock;
    }

    public void setControlSock(Socket subSock) {
        this.ctrlSock = subSock;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String id) {
        this.identity = id;
    }
}
