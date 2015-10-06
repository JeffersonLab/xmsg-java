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

package org.jlab.coda.xmsg.xsys.regdis;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.util.Arrays;

/**
 * A wrapper for a a registration request.
 *
 * @author gurjyan
 * @since 2.x
 */
class xMsgRegRequest {

    private final String topic;
    private final String sender;
    private final byte[] data;

    /**
     * Constructs registration request using xMsgRegistration data object
     *
     * @param topic the request being responded
     * @param sender the sender of the response
     * @param data the registration data of the request:
     *             object of {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration}
     */
    public xMsgRegRequest(String topic, String sender, xMsgRegistration data) {
        this.topic = topic;
        this.sender = sender;
        this.data = data.toByteArray();
    }

    /**
     * Constructs registration request using String representation of the request
     *
     * @param topic the request being responded
     * @param sender the sender of the response
     * @param text the registration text of the request
     */
    public xMsgRegRequest(String topic, String sender, String text) {
        this.topic = topic;
        this.sender = sender;
        this.data = text.getBytes();
    }

    /**
     * De-serializes the request from the given message.
     *
     * @param msg the message with the response
     * @throws xMsgException
     */
    public xMsgRegRequest(ZMsg msg) throws xMsgException {

        if (msg.size() != 3) {
            throw new xMsgException("xMsg-Error: registration message format violation");
        }

        ZFrame topicFrame = msg.pop();
        ZFrame senderFrame = msg.pop();
        ZFrame dataFrame = msg.pop();

        try {
            topic = new String(topicFrame.getData());
            sender = new String(senderFrame.getData());
            data = dataFrame.getData();
        } finally {
            senderFrame.destroy();
            topicFrame.destroy();
        }
    }

    /**
     * Serializes the request into a message.
     *
     * @return a message containing the request
     */
    public ZMsg msg() {
        ZMsg msg = new ZMsg();
        msg.addString(topic);
        msg.addString(sender);
        msg.add(data);
        return msg;
    }

    /**
     * Returns the topic of the request.
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the sender of the request.
     */
    public String sender() {
        return sender;
    }

    /**
     * Returns the data of the request.
     *
     * @throws InvalidProtocolBufferException when the data is corrupted
     */
    public xMsgRegistration data() throws InvalidProtocolBufferException {
        return xMsgRegistration.parseFrom(data);
    }

    /**
     * Returns the text of the registration request.
     */
    public String text() {
        return new String(data);
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(data);
        result = prime * result + sender.hashCode();
        result = prime * result + topic.hashCode();
        return result;
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
        xMsgRegRequest other = (xMsgRegRequest) obj;
        if (!Arrays.equals(data, other.data)) {
            return false;
        }
        if (!sender.equals(other.sender)) {
            return false;
        }
        if (!topic.equals(other.topic)) {
            return false;
        }
        return true;
    }
}
