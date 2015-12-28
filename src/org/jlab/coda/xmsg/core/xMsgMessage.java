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
 * <p>
 * Uses {@link xMsgData} class generated as a result of the proto-buffer
 * description to pass Java primitive types and arrays of primitive types.
 * xMsgData is also used to pass byte[]: the result of a user specific object
 * serialization.
 * <p>
 * This class will also contain complete metadata of the message data,
 * describing details of the data. In case a message is constructed
 * without metadata, the default metadata will be created with only the
 * data type set.
 *
 * @author gurjyan
 * @version 2.x
 */
public class xMsgMessage {

    // topic of the message
    private xMsgTopic topic;

    // metadata of the message
    private xMsgMeta.Builder metaData;

    // data of the message
    private byte[] data;


    /**
     * Constructs a message containing a byte[] that is most likely is the result of
     * user serialization. Thus, user also provides a metadata describing the
     * type of the data among other things.
     *
     * @param topic    the topic of the message
     * @param metaData the metadata of the message, describing the data
     * @param data     serialized data
     */
    public xMsgMessage(xMsgTopic topic, xMsgMeta.Builder metaData, byte[] data) {
        this.topic = topic;
        this.metaData = metaData;
        this.data = data;
    }

    /**
     * Constructs a message containing a byte[] that is most likely is the result of
     * user serialization. Thus, user also provides a mime-type describing the
     * type of the data.
     *
     * @param topic the topic of the message
     * @param mimeType user textual definition of the data type
     * @param data data object
     */
    public xMsgMessage(xMsgTopic topic, String mimeType, byte[] data) {
        this.topic = topic;
        this.metaData = xMsgMeta.newBuilder();
        this.metaData.setDataType(mimeType);
        this.data = data;
    }

    /**
     *  Create xMsgMessage from the 0MQ message received off the wire, i.e.
     *  de-serializes the received 0MQ message
     *
     * @param msg the received message
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
            throw new xMsgException("xMsg-Error: Could not parse metadata", e);
        } finally {
            topicFrame.destroy();
            metaDataFrame.destroy();
            dataFrame.destroy();
        }
    }

    /**
     * Serializes this message into a 0MQ message,
     * ready to send it over the wire.
     *
     * @return the raw multi-part message
     */
    ZMsg serialize() {
        ZMsg msg = new ZMsg();
        msg.add(topic.toString());
        msg.add(metaData.build().toByteArray());
        msg.add(data);
        return msg;
    }

    /**
     * Returns the topic of the message.
     */
    public xMsgTopic getTopic() {
        return topic;
    }

    /**
     * Returns the metadata of the message.
     */
    public xMsgMeta.Builder getMetaData() {
        return metaData;
    }

    /**
     * Returns the size of the message data (i.e. serialized byte[] ).
     */
    public int getDataSize() {
        return data != null ? data.length : 0;
    }

    /**
     * Returns the data of the message.
     */
    public byte[] getData() {
        return data;
    }

    public void setTopic(xMsgTopic topic) {
        this.topic = topic;
    }

    public void setMetaData(xMsgMeta.Builder metaData) {
        this.metaData = metaData;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public xMsgMessage response() throws xMsgException, IOException {
        xMsgTopic resTopic = xMsgTopic.wrap(metaData.getReplyTo());
        xMsgMessage res = createFrom(resTopic, data);
        res.getMetaData().mergeFrom(metaData.build());
        res.getMetaData().clearReplyTo();
        return res;
    }


    public xMsgMessage response(Object data) throws xMsgException, IOException {
        xMsgTopic resTopic = xMsgTopic.wrap(metaData.getReplyTo());
        xMsgMessage res = createFrom(resTopic, data);
        res.getMetaData().mergeFrom(metaData.build());
        res.getMetaData().clearReplyTo();
        return res;
    }

    /**
     * Constructs a message, data of which is passed as an Object. This method will
     * do it's best to figure out the type of the object, updating accordingly the
     * data mimeType. It will also serialize the object and store it as a byte[].
     * Note that this will fail in case the passed object is not serializable.
     * So, serializable java objects will be serialized and metadata dataType will
     * be assigned to the mimeType = "binary/java".
     *
     * @param topic the topic of the message
     * @param data the data object
     * @throws IOException
     */
    public static xMsgMessage createFrom(xMsgTopic topic, Object data)
            throws IOException {

        byte[] ba = null;
        String mimeType = null;
        xMsgData.Builder xd = xMsgData.newBuilder();

        if (data instanceof Integer) {
            mimeType = xMsgConstants.MimeType.SFIXED32;
            xd.setFLSINT32((Integer) data);

        } else if (data instanceof Long) {
            mimeType = xMsgConstants.MimeType.SFIXED64;
            xd.setFLSINT64((Long) data);

        } else if (data instanceof Float) {
            mimeType = xMsgConstants.MimeType.FLOAT;
            xd.setFLOAT((Float) data);

        } else if (data instanceof Double) {
            mimeType = xMsgConstants.MimeType.DOUBLE;
            xd.setDOUBLE((Double) data);

        } else if (data instanceof String) {
            mimeType = xMsgConstants.MimeType.STRING;
            xd.setSTRING((String) data);

        } else if (data instanceof Integer[]) {
            mimeType = xMsgConstants.MimeType.ARRAY_SFIXED32;
            xd.addAllFLSINT32A(Arrays.asList((Integer[]) data));

        } else if (data instanceof Long[]) {
            mimeType = xMsgConstants.MimeType.ARRAY_SFIXED64;
            xd.addAllFLSINT64A(Arrays.asList((Long[]) data));

        } else if (data instanceof Float[]) {
            mimeType = xMsgConstants.MimeType.ARRAY_FLOAT;
            xd.addAllFLOATA(Arrays.asList((Float[]) data));

        } else if (data instanceof Double[]) {
            mimeType = xMsgConstants.MimeType.ARRAY_DOUBLE;
            xd.addAllDOUBLEA(Arrays.asList((Double[]) data));

        } else if (data instanceof String[]) {
            mimeType = xMsgConstants.MimeType.ARRAY_STRING;
            xd.addAllSTRINGA(Arrays.asList((String[]) data));

        } else if (data instanceof byte[]) {
            mimeType = xMsgConstants.MimeType.BYTES;
            ba = (byte[]) data;

        } else {
            mimeType = xMsgConstants.MimeType.JOBJECT;
            ba = xMsgUtil.serializeToBytes(data);
        }

        if (ba == null) {
            ba = xd.build().toByteArray();
        }

        return new xMsgMessage(topic, mimeType, ba);
    }
}
