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

import java.util.HashSet;
import java.util.Set;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A wrapper for a response to a registration or discovery request.
 * <p>
 * A response of the {@link xMsgRegService registration service} can be an
 * string indicating that the request was successful, a set of registration data
 * in case a discovery request was received, or an error description indicating
 * that something wrong happened with the request.
 */
public class xMsgRegResponse {

    private final String topic;
    private final String sender;
    private final String status;
    private final Set<xMsgRegistration> data;

    /**
     * Constructs a success response. No registration data is returned.
     * The response status is set to
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#SUCCESS}.
     * This response is used to signal that a request was successful.
     *
     * @param topic the request being responded
     * @param sender the sender of the response
     */
    public xMsgRegResponse(String topic, String sender) {
        this.topic = topic;
        this.sender = sender;
        this.status = xMsgConstants.SUCCESS.toString();
        this.data = new HashSet<xMsgRegistration>();
    }


    /**
     * Constructs a data response. The data can be an empty set.
     * The response status is set to
     * {@link org.jlab.coda.xmsg.core.xMsgConstants#SUCCESS}.
     * This response is used to return registration data for discovery requests.
     *
     * @param topic the request being responded
     * @param sender the sender of the response
     * @param data the registration data
     */
    public xMsgRegResponse(String topic, String sender, Set<xMsgRegistration> data) {
        this.topic = topic;
        this.sender = sender;
        this.status = xMsgConstants.SUCCESS.toString();
        this.data = data;
    }


    /**
     * Constructs an error response. No registration data is returned.
     *
     * @param topic the request being responded
     * @param sender the sender of the response
     * @param errorMsg the error description
     */
    public xMsgRegResponse(String topic, String sender, String errorMsg) {
        this.topic = topic;
        this.sender = sender;
        this.status = errorMsg;
        this.data = new HashSet<>();
    }


    /**
     * Deserializes the response from the given message.
     *
     * @param msg the message with the response
     * @throws xMsgRegistrationException
     *         when the message is malformed or the data is corrupted,
     *         or when the response is an error
     *         (and the exception message is set to the error description)
     */
    public xMsgRegResponse(ZMsg msg) throws xMsgRegistrationException {

        if (msg.size() < 3) {
            throw new xMsgRegistrationException("xMsg message format violation");
        }

        ZFrame topicFrame = msg.pop();
        ZFrame senderFrame = msg.pop();
        ZFrame statusFrame = msg.pop();

        try {
            topic = new String(topicFrame.getData());
            sender = new String(senderFrame.getData());
            status = new String(statusFrame.getData());
            if (!status.equals(xMsgConstants.SUCCESS.toString())) {
                throw new xMsgRegistrationException(status);
            }

            data = new HashSet<>();
            while (!msg.isEmpty()) {
                ZFrame dataFrame = msg.pop();
                try {
                    data.add(xMsgRegistration.parseFrom(dataFrame.getData()));
                } catch (InvalidProtocolBufferException e) {
                    throw new xMsgRegistrationException("Could not deserialize data");
                } finally {
                    dataFrame.destroy();
                }
            }
        } finally {
            statusFrame.destroy();
            senderFrame.destroy();
            topicFrame.destroy();
        }
    }


    /**
     * Serializes the response into a message.
     *
     * @return a message containing the response
     */
    public ZMsg msg() {
        ZMsg msg = new ZMsg();
        msg.addString(topic);
        msg.addString(sender);
        msg.addString(status);
        for (xMsgRegistration d : data) {
            msg.add(d.toByteArray());
        }
        return msg;
    }


    /**
     * Returns the topic of the response.
     */
    public String topic() {
        return topic;
    }


    /**
     * Returns the sender of the response.
     */
    public String sender() {
        return sender;
    }


    /**
     * Returns the status of the response.
     * It can be {@link org.jlab.coda.xmsg.core.xMsgConstants#SUCCESS}
     * or an error string indicating a problem with the request.
     */
    public String status() {
        return status;
    }


    /**
     * Returns the data of the response.
     * When the response is for indicating a success or error, the data is
     * empty. It can also be empty when no registration data is found for the
     * given request.
     */
    public Set<xMsgRegistration> data() {
        return data;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + data.hashCode();
        result = prime * result + sender.hashCode();
        result = prime * result + status.hashCode();
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
        xMsgRegResponse other = (xMsgRegResponse) obj;
        if (!data.equals(other.data)) {
            return false;
        }
        if (!sender.equals(other.sender)) {
            return false;
        }
        if (!status.equals(other.status)) {
            return false;
        }
        if (!topic.equals(other.topic)) {
            return false;
        }
        return true;
    }
}
