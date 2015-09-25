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

import java.io.IOException;
import java.util.Arrays;

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
 */
public class xMsgMessage {

    private xMsgTopic topic;
    private xMsgMeta.Builder metaData;
    private byte[] data;


    /**
     *
     * @param topic
     * @param metaData
     * @param data
     * @throws IOException
     */
    public xMsgMessage(xMsgTopic topic, xMsgMeta.Builder metaData, Object data) throws IOException {
        _construct(topic, metaData, data);
    }

    /**
     *
     * @param topic
     * @param mimeType
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    public xMsgMessage(xMsgTopic topic, String mimeType, Object data) throws xMsgException, IOException {
        xMsgMeta.Builder md = xMsgMeta.newBuilder();
        md.setDataType(mimeType);
        _construct(topic, md, data);
    }

    /**
     *
     * @param topic
     * @param data
     * @throws xMsgException
     * @throws IOException
     */
    public xMsgMessage(xMsgTopic topic, Object data) throws xMsgException, IOException {
        this(topic, "binary/bytes", data);
    }

    /**
     * <p>
     *     Create xMsgMessage off the wire, i.e.
     *     de-serializes the received ZMsg message
     * </p>
     *
     * @param msg the received ZMQ message
     */
    public xMsgMessage(ZMsg msg) throws xMsgException {
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
     * <p>
     *    Serializes this message into a ZMsg message,
     *    to send it over the wire.
     * </p>
     *
     * @return the ZMsg raw multi-part message
     */
    public ZMsg serialize() {
        ZMsg msg = new ZMsg();
        msg.add(topic.toString());
        msg.add(metaData.build().toByteArray());
        msg.add(data);
        return msg;
    }


    /**
     *
     * @return
     */
    public int getDataSize() {
        return data != null ? data.length : 0;
    }

    /**
     * @return
     */
    public xMsgTopic getTopic() {
        return topic;
    }

    /**
     *
     * @param topic
     */
    public void setTopic(xMsgTopic topic) {
        this.topic = topic;
    }

    /**
     *
     * @return
     */
    public xMsgMeta.Builder getMetaData() {
        return metaData;
    }

    /**
     *
     * @param metaData
     */
    public void setMetaData(xMsgMeta.Builder metaData) {
        this.metaData = metaData;
    }

    /**
     *
     * @return
     */
    public byte[] getData() {
        return data;
    }

    /**
     *
     * @param topic
     * @param metaData
     * @param data
     * @throws IOException
     */
    private void _construct(xMsgTopic topic, xMsgMeta.Builder metaData, Object data) throws IOException {
        this.topic = topic;
        byte[] ba = null;

        String mimeType = metaData.getDataType();

        xMsgData.Builder xd = xMsgData.newBuilder();

        // define the data mime-type
        if (data instanceof Integer) {
            mimeType = xMsgConstants.SFIXED32.getStringValue();
            xd.setFLSINT32((Integer) data);

        } else if (data instanceof Long) {
            mimeType = xMsgConstants.SFIXED64.getStringValue();
            xd.setFLSINT64((Long) data);

        } else if (data instanceof Float) {
            mimeType = xMsgConstants.FLOAT.getStringValue();
            xd.setFLOAT((Float) data);

        } else if (data instanceof Double) {
            mimeType = xMsgConstants.DOUBLE.getStringValue();
            xd.setDOUBLE((Double) data);

        } else if (data instanceof String) {
            mimeType = xMsgConstants.STRING.getStringValue();
            xd.setSTRING((String) data);

        } else if (data instanceof Integer[]) {
            mimeType = xMsgConstants.ARRAY_SFIXED32.getStringValue();
            xd.addAllFLSINT32A(Arrays.asList((Integer[]) data));

        } else if (data instanceof Long[]) {
            mimeType = xMsgConstants.ARRAY_SFIXED64.getStringValue();
            xd.addAllFLSINT64A(Arrays.asList((Long[]) data));

        } else if (data instanceof Float[]) {
            mimeType = xMsgConstants.ARRAY_FLOAT.getStringValue();
            xd.addAllFLOATA(Arrays.asList((Float[]) data));

        } else if (data instanceof Double[]) {
            mimeType = xMsgConstants.ARRAY_DOUBLE.getStringValue();
            xd.addAllDOUBLEA(Arrays.asList((Double[]) data));

        } else if (data instanceof String[]) {
            mimeType = xMsgConstants.ARRAY_STRING.getStringValue();
            xd.addAllSTRINGA(Arrays.asList((String[]) data));

        } else if (data instanceof byte[]) {
            ba = (byte[]) data;
        } else {
            ba = xMsgUtil.serializeToBytes(data);
        }

        metaData.setDataType(mimeType);
        this.metaData = metaData;
        if (ba != null) {
            this.data = ba;
        } else {
            this.data = xd.build().toByteArray();
        }
    }

}
