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

package org.jlab.coda.xmsg.core;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;
import org.jlab.coda.xmsg.data.xMsgMimeType;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * The user-data message for xMsg pub/sub communications.
 * <p>
 * An xMsg message is composed of a <i>topic</i>, <i>metadata</i> and binary data.
 * The {@link xMsgTopic topic} to which the message is published will be used by
 * subscribers to filter messages of interest.
 * The {@link xMsgMeta metadata} is a protobuf class with fields that can be
 * used to describe the data of the message and the communication request the
 * message is part of. At minimum, the {@code dataType} field is required to
 * indicate the mime-type of the binary data.
 * The data byte array contains the binary representation of the actual data of
 * the message. xMsg provides helpers to serialize primitive data types. Complex
 * objects must be serialized before creating the message
 * (i.e. applications using xMsg must take care of the binary data format).
 * <p>
 * When a message is sync-published, the <i>metadata</i> will contain an
 * auto-generated {@code replyTopic} where the response can be published to.
 *
 * @version 2.x
 */
public class xMsgMessage {

    private final xMsgTopic topic;
    private final xMsgMeta.Builder metaData;
    private final byte[] data;

    /**
     * Constructs a new message.
     * The message will be published to the given topic.
     * The metadata must contain the mime-type describing the data.
     * <p>
     * The byte array will be owned by the message, thus, it cannot be modified
     * by the calling code after creating this message.
     * If the byte array must be reused or modified, pass the array to the
     * constructor as {@code data.clone()} to ensure the message keeps a copy of
     * the data and not the original array.
     *
     * @param topic    the topic of the message
     * @param metaData the metadata of the message
     * @param data     serialized data
     */
    public xMsgMessage(xMsgTopic topic, xMsgMeta.Builder metaData, byte[] data) {
        this.topic = topic;
        this.metaData = metaData;
        this.data = data;
    }

    /**
     * Constructs a new message.
     * The message will be published to the given topic.
     * The metadata will only contain the specified {@code mime-type} describing
     * the data.
     * <p>
     * The byte array will be owned by the message, thus, it cannot be modified
     * by the calling code after creating this message.
     * If the byte array must be reused or modified, pass the array to the
     * constructor as {@code data.clone()} to ensure the message keeps a copy of
     * the data and not the original array.
     *
     * @param topic    the topic of the message
     * @param mimeType the mime-type string for the data
     * @param data     serialized data
     */
    public xMsgMessage(xMsgTopic topic, String mimeType, byte[] data) {
        this.topic = topic;
        this.metaData = xMsgMeta.newBuilder();
        this.metaData.setDataType(mimeType);
        this.data = data;
    }

    /**
     *  Creates a message from the 0MQ message received from the wire.
     *
     * @param msg the received 0MQ message
     */
    xMsgMessage(ZMsg msg) throws xMsgException {

        if (msg.size() != 3) {
            throw new xMsgException("invalid pub/sub message format");
        }

        ZFrame topicFrame = msg.pop();
        ZFrame metaDataFrame = msg.pop();
        ZFrame dataFrame = msg.pop();

        try {
            this.topic = xMsgTopic.wrap(topicFrame.getData());
            xMsgMeta metaDataObj = xMsgMeta.parseFrom(metaDataFrame.getData());
            this.metaData = metaDataObj.toBuilder();
            this.data = dataFrame.getData();
        } catch (InvalidProtocolBufferException e) {
            throw new xMsgException("could not parse metadata", e);
        }
    }

    /**
     * Serializes this message into a 0MQ message,
     * ready to send it over the wire.
     *
     * @return the 0MQ message
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
     *
     * @return the topic to which the message is published
     */
    public xMsgTopic getTopic() {
        return topic;
    }

    /**
     * Returns the metadata of the message.
     *
     * @return a reference to the metadata of the message
     */
    public xMsgMeta.Builder getMetaData() {
        return metaData;
    }

    /**
     * Returns the mime-type of the message data.
     *
     * @return a string with the mime-type
     */
    public String getMimeType() {
        return metaData.getDataType();
    }

    /**
     * Checks if the message has a reply topic.
     * If true, the message is part of a sync-publish request and a response is
     * expected to be published to the reply topic.
     *
     * @return true if the message was sent as a sync-publish request, false otherwise
     */
    public boolean hasReplyTopic() {
        return metaData.hasReplyTo();
    }

    /**
     * Returns the topic this message should be replied to.
     *
     * @return the topic to publish the response for this message
     */
    public xMsgTopic getReplyTopic() {
        return xMsgTopic.wrap(metaData.getReplyTo());
    }

    /**
     * Checks if the metadata contains byte order information.
     *
     * @return true if the data must be used with a certain byte-order
     */
    public boolean hasDataOrder() {
        return metaData.hasByteOrder();
    }

    /**
     * Returns the byte order of the data, if set.
     * If the byte order is not set in the metadata,
     * the returned value is {@link ByteOrder#BIG_ENDIAN BIG_ENDIAN}.
     *
     * @return the byte-order on which the data must be accessed
     */
    public ByteOrder getDataOrder() {
        if (!metaData.hasByteOrder()) {
            return ByteOrder.BIG_ENDIAN;
        }
        switch (metaData.getByteOrder()) {
            case Big:
                return ByteOrder.BIG_ENDIAN;
            case Little:
                return ByteOrder.LITTLE_ENDIAN;
            default:
                throw new RuntimeException("invalid byte order: " + metaData.getByteOrder());
        }
    }

    /**
     * Returns the size of the byte array containing the data.
     *
     * @return the size of the data, in bytes
     */
    public int getDataSize() {
        return data != null ? data.length : 0;
    }

    /**
     * Returns the data of the message.
     *
     * @return the byte array with the raw message data
     */
    public byte[] getData() {
        return data;
    }


    /**
     * Constructs a message with the given data.
     * <p>
     * This method will do it's best to figure out the type of the object,
     * updating accordingly the data mime-type.
     * It will also serialize the object and store it as a byte array.
     * <p>
     * In case of passing a Java object, this will fail if the object is not
     * serializable.
     *
     * @param topic the topic of the message
     * @param data the data of the message
     * @return a new message containing the given data
     * @throws UncheckedIOException if data is a Java object and serialization failed
     * @see xMsgMimeType
     */
    public static xMsgMessage createFrom(xMsgTopic topic, Object data) {

        byte[] ba = null;
        final String mimeType;
        xMsgData.Builder xd = xMsgData.newBuilder();

        if (data instanceof String) {
            mimeType = xMsgMimeType.STRING;
            xd.setSTRING((String) data);

        } else if (data instanceof Integer) {
            mimeType = xMsgMimeType.SFIXED32;
            xd.setFLSINT32((Integer) data);

        } else if (data instanceof Long) {
            mimeType = xMsgMimeType.SFIXED64;
            xd.setFLSINT64((Long) data);

        } else if (data instanceof Float) {
            mimeType = xMsgMimeType.FLOAT;
            xd.setFLOAT((Float) data);

        } else if (data instanceof Double) {
            mimeType = xMsgMimeType.DOUBLE;
            xd.setDOUBLE((Double) data);

        } else if (data instanceof String[]) {
            mimeType = xMsgMimeType.ARRAY_STRING;
            xd.addAllSTRINGA(Arrays.asList((String[]) data));

        } else if (data instanceof Integer[]) {
            mimeType = xMsgMimeType.ARRAY_SFIXED32;
            xd.addAllFLSINT32A(Arrays.asList((Integer[]) data));

        } else if (data instanceof Long[]) {
            mimeType = xMsgMimeType.ARRAY_SFIXED64;
            xd.addAllFLSINT64A(Arrays.asList((Long[]) data));

        } else if (data instanceof Float[]) {
            mimeType = xMsgMimeType.ARRAY_FLOAT;
            xd.addAllFLOATA(Arrays.asList((Float[]) data));

        } else if (data instanceof Double[]) {
            mimeType = xMsgMimeType.ARRAY_DOUBLE;
            xd.addAllDOUBLEA(Arrays.asList((Double[]) data));

        } else if (data instanceof byte[]) {
            mimeType = xMsgMimeType.BYTES;
            ba = (byte[]) data;

        } else {
            mimeType = xMsgMimeType.JOBJECT;
            try {
                ba = xMsgUtil.serializeToBytes(data);
            } catch (IOException e) {
                throw new UncheckedIOException("could not serialize object", e);
            }
        }

        if (ba == null) {
            ba = xd.build().toByteArray();
        }

        return new xMsgMessage(topic, mimeType, ba);
    }


    /**
     * Deserializes data from the given message.
     *
     * @param message the message that contains the required data
     * @return the deserialized data of the message
     */
    public static Object parseData(xMsgMessage message) {
        try {
            byte[] data = message.getData();
            String dataType = message.getMimeType();

            if (dataType.equals(xMsgMimeType.STRING)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasSTRING()) {
                    return xd.getSTRING();
                }

            } else if (dataType.equals(xMsgMimeType.SFIXED32)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasFLSINT32()) {
                    return xd.getFLSINT32();
                }

            } else if (dataType.equals(xMsgMimeType.SFIXED64)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasFLSINT64()) {
                    return xd.getFLSINT64();
                }

            } else if (dataType.equals(xMsgMimeType.FLOAT)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasFLOAT()) {
                    return xd.getFLOAT();
                }

            } else if (dataType.equals(xMsgMimeType.DOUBLE)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasDOUBLE()) {
                    return xd.getDOUBLE();
                }

            } else if (dataType.equals(xMsgMimeType.ARRAY_STRING)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<String> list = xd.getSTRINGAList();
                if (!list.isEmpty()) {
                    return list.toArray(new String[0]);
                }

            } else if (dataType.equals(xMsgMimeType.ARRAY_SFIXED32)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Integer> list = xd.getFLSINT32AList();
                if (!list.isEmpty()) {
                    return list.toArray(new Integer[0]);
                }

            } else if (dataType.equals(xMsgMimeType.ARRAY_SFIXED64)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Long> list = xd.getFLSINT64AList();
                if (!list.isEmpty()) {
                    return list.toArray(new Long[0]);
                }

            } else if (dataType.equals(xMsgMimeType.ARRAY_FLOAT)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Float> list = xd.getFLOATAList();
                if (!list.isEmpty()) {
                    return list.toArray(new Float[0]);
                }

            } else if (dataType.equals(xMsgMimeType.ARRAY_DOUBLE)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Double> list = xd.getDOUBLEAList();
                if (!list.isEmpty()) {
                    return list.toArray(new Double[0]);
                }

            } else {
                try {
                    return xMsgUtil.deserialize(data);
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException("could not deserialize data", e);
                }
            }

            throw new IllegalArgumentException("the message data doesn't match the mime-type:"
                                               + dataType);

        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("message doesn't contain a valid xMsg data buffer");
        }
    }


    /**
     * Deserializes simple data from the given message.
     * Useful when the message was created with {@link #createFrom}.
     * The message should contain data of the given {@code dataType}.
     *
     * @param <T> the expected type of the data
     * @param message the message that contains the required data
     * @param dataType the type of the required data
     * @return the deserialized data of the message,
     *         if the message contains data of the given type
     * @throws IllegalArgumentException if the message does not contain data
     *         of the expected type
     * @see xMsgMimeType
     */
    public static <T> T parseData(xMsgMessage message, Class<T> dataType) {
        try {
            byte[] data = message.getData();

            if (dataType.equals(String.class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasSTRING()) {
                    return dataType.cast(xd.getSTRING());
                }

            } else if (dataType.equals(Integer.class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasFLSINT32()) {
                    return dataType.cast(xd.getFLSINT32());
                }

            } else if (dataType.equals(Long.class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasFLSINT64()) {
                    return dataType.cast(xd.getFLSINT64());
                }

            } else if (dataType.equals(Float.class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasFLOAT()) {
                    return dataType.cast(xd.getFLOAT());
                }

            } else if (dataType.equals(Double.class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasDOUBLE()) {
                    return dataType.cast(xd.getDOUBLE());
                }

            } else if (dataType.equals(String[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<String> list = xd.getSTRINGAList();
                if (!list.isEmpty()) {
                    String[] array = list.toArray(new String[0]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Integer[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Integer> list = xd.getFLSINT32AList();
                if (!list.isEmpty()) {
                    Integer[] array = list.toArray(new Integer[0]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Long[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Long> list = xd.getFLSINT64AList();
                if (!list.isEmpty()) {
                    Long[] array = list.toArray(new Long[0]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Float[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Float> list = xd.getFLOATAList();
                if (!list.isEmpty()) {
                    Float[] array = list.toArray(new Float[0]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Double[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Double> list = xd.getDOUBLEAList();
                if (!list.isEmpty()) {
                    Double[] array = list.toArray(new Double[0]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Object.class)) {
                try {
                    return dataType.cast(xMsgUtil.deserialize(data));
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException("could not deserialize data", e);
                }
            }

            throw new IllegalArgumentException("message doesn't contain data of type: " + dataType);

        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("message doesn't contain a valid xMsg data buffer");
        }
    }


    /**
     * Creates a response to the given message, using the same data.
     * The message must contain the <i>replyTo</i> metadata field.
     *
     * @param msg the received message to be responded
     * @return a response message with the proper topic and the same received data
     */
    public static xMsgMessage createResponse(xMsgMessage msg) {
        xMsgTopic resTopic = xMsgTopic.wrap(msg.metaData.getReplyTo());
        xMsgMeta.Builder resMeta = xMsgMeta.newBuilder(msg.metaData.build());
        resMeta.clearReplyTo();
        return new xMsgMessage(resTopic, resMeta, msg.data);
    }

    /**
     * Creates a response to the given message, serializing the given data.
     * The message must contain the <i>replyTo</i> metadata field.
     *
     * @param msg the received message to be responded
     * @param data the data to be sent back
     * @return a response message with the proper topic and the given data
     */
    public static xMsgMessage createResponse(xMsgMessage msg, Object data) {
        xMsgTopic resTopic = xMsgTopic.wrap(msg.metaData.getReplyTo());
        return createFrom(resTopic, data);
    }
}
