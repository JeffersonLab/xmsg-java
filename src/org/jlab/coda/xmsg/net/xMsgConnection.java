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

import org.zeromq.ZMQ.Socket;

/**
 * The standard connection to xMsg nodes.
 * Contains xMSgAddress object and two 0MQ sockets for publishing and
 * subscribing xMsg messages respectfully.
 *
 * @author gurjyan
 * @since 1.0
 */
public class xMsgConnection {

    private xMsgAddress address;
    private Socket pubSock;
    private Socket subSock;


    public xMsgAddress getAddress() {
        return address;
    }

    public void setAddress(xMsgAddress address) {
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
}
