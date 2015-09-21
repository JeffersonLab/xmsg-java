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

package org.jlab.coda.xmsg.core;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

/**
 * Defines a message to be passed through 0MQ.
 *
 * Uses {@link xMsgData} class generated as a result of the proto-buffer
 * description to pass Java primitive types and arrays of primitive types.
 * xMsgData is also used to pass byte[]: the result of a user specific
 * object serialization.
 * <p>
 * This class will also contain complete metadata of the message data,
 * describing details of the data. In case a message is constructed
 * without metadata, the default metadata will be created, and only the data
 * type will be set.
 *
 * @author gurjyan
 * @version 2.x
 * @since 11/5/14
 */
public class xMsgMessage {

    private xMsgTopic topic;
    private xMsgMeta.Builder metaData;
    private byte[] data;

    /**
     * Constructs a message with empty data.
     *
     * @param topic the topic of the message
     */
    public xMsgMessage(xMsgTopic topic) {
        this.topic = topic;
        this.metaData = xMsgMeta.newBuilder();
    }

    /**
     * Constructs a message with full metadata and native xMsg data.
     * Use {@link xMsgData} as a helper to send a message with primitive data
     * that can be read by any xMsg implementation natively.
     * This data will be serialized and then stored in the message.
     *
     * @param topic the topic of the message
     * @param metaData the full metadata of the message
     * @param data the native data of the message
     */
    public xMsgMessage(xMsgTopic topic, xMsgMeta.Builder metaData, xMsgData data) {
        this.topic = topic;
        this.metaData = metaData;
        this.data = data.toByteArray();
    }

    /**
     * Constructs a message with only raw data and default metadata.
     * The mime-type of the data.
     *
     * @param topic the topic of the message
     * @param mimeType the mime-type of the data
     * @param data the raw data of the message
     */
    public xMsgMessage(xMsgTopic topic, String mimeType, byte[] data) {
        this.metaData = xMsgMeta.newBuilder();
        this.metaData.setDataType(mimeType);
        this.data = data;
    }

    /**
     * Constructs a message with full metadata and raw data.
     *
     * @param topic the topic of the message
     * @param metaData the full metadata of the message
     * @param data the raw data of the message
     */
    public xMsgMessage(xMsgTopic topic, xMsgMeta.Builder metaData, byte[] data) {
        this.topic = topic;
        this.metaData = metaData;
        this.data = data;
    }


    /**
     * Constructs a message with string data.
     *
     * @param topic the topic of the message
     * @param text the data of the message
     */
    public xMsgMessage(xMsgTopic topic, String text) {
        this.topic = topic;
        this.metaData = xMsgMeta.newBuilder();
        this.metaData.setDataType("text/string");
        this.data = text.getBytes();
    }


    /**
     * Deserializes a received message.
     *
     * @param msg the received ZMQ message
     */
    xMsgMessage(ZMsg msg) throws xMsgException {
        ZFrame topicFrame = msg.pop();
        ZFrame metaDataFrame = msg.pop();
        ZFrame dataFrame = msg.pop();

        try {
            this.topic = xMsgTopic.wrap(topicFrame.getData());
            xMsgMeta metaDataObj = xMsgMeta.parseFrom(metaDataFrame.getData());
            this.metaData = metaDataObj.toBuilder();
            this.data = dataFrame.getData();
        } catch (InvalidProtocolBufferException e) {
            throw new xMsgException("Could not parse metadata", e);
        } finally {
            topicFrame.destroy();
            metaDataFrame.destroy();
            dataFrame.destroy();
        }
    }

    /**
     * Serializes this message into a ZMQ message.
     *
     * @return the ZMQ raw multi-part message
     */
    ZMsg serialize() {
        ZMsg msg = new ZMsg();
        msg.add(topic.toString());
        msg.add(metaData.build().toByteArray());
        msg.add(data);
        return msg;
    }

    /**
     * Returns the topic of this message.
     */
    public xMsgTopic getTopic() {
        return topic;
    }

    /**
     * Sets the topic of this message.
     *
     * @param topic the topic
     */
    public void setTopic(xMsgTopic topic) {
        this.topic = topic;
    }

    /**
     * Returns the metadata of this message.
     */
    public xMsgMeta.Builder getMetaData() {
        return metaData;
    }

    /**
     * Sets the metadata of this message.
     * This will overwrite any mime-type already set.
     *
     * @param metaData the metatada
     */
    public void setMetaData(xMsgMeta.Builder metaData) {
        this.metaData.mergeFrom(metaData.build());
    }

    /**
     * Returns the mime-type of the message data.
     */
    public String getMimeType() {
        return metaData.getDataType();
    }

    /**
     * Returns the message data.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Sets the message data.
     * This data will be serialized and then stored in the message.
     *
     * @param data the native data of the message
     */
    public void setData(xMsgData data) {
        this.metaData.setDataType("native");
        this.data = data.toByteArray();
    }

    /**
     * Sets an string as the message data.
     *
     * @param text the data of the message
     */
    public void setData(String text) {
        this.metaData.setDataType("text/string");
        this.data = text.getBytes();
    }

    /**
     * Returns the size of the message data.
     */
    public int getDataSize() {
        return data != null ? data.length : 0;
    }

    /**
     * Sets the message data.
     *
     * @param mimeType the mime-type of the data
     * @param data     the raw data of the message
     */
    public void setData(String mimeType, byte[] data) {
        this.metaData.setDataType(mimeType);
        this.data = data;
    }
}
