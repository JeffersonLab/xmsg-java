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
import java.util.Arrays;
import java.util.List;

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
    private final xMsgTopic topic;

    // metadata of the message
    private final xMsgMeta.Builder metaData;

    // data of the message
    private final byte[] data;


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

        if (msg.size() != 3) {
            throw new xMsgException("xMsg-Error: invalid pub/sub message format");
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
     * @throws UncheckedIOException if data is a Java object and serialization failed
     */
    public static xMsgMessage createFrom(xMsgTopic topic, Object data) {

        byte[] ba = null;
        final String mimeType;
        xMsgData.Builder xd = xMsgData.newBuilder();

        if (data instanceof Integer) {
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

        } else if (data instanceof String) {
            mimeType = xMsgMimeType.STRING;
            xd.setSTRING((String) data);

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

        } else if (data instanceof String[]) {
            mimeType = xMsgMimeType.ARRAY_STRING;
            xd.addAllSTRINGA(Arrays.asList((String[]) data));

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
     * Deserializes simple data from the given message.
     *
     * @param message the message that contains the required data
     * @param dataType the type of the required data
     * @return the deserialized data of the message
     * @throws xMsgException if the message data is not of the given type
     */
    public static <T> T parseData(xMsgMessage message, Class<T> dataType) {
        try {
            byte[] data = message.getData();

            if (dataType.equals(Integer.class)) {
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

            } else if (dataType.equals(String.class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                if (xd.hasSTRING()) {
                    return dataType.cast(xd.getSTRING());
                }

            } else if (dataType.equals(Integer[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Integer> list = xd.getFLSINT32AList();
                if (!list.isEmpty()) {
                    Integer[] array = list.toArray(new Integer[list.size()]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Long[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Long> list = xd.getFLSINT64AList();
                if (!list.isEmpty()) {
                    Long[] array = list.toArray(new Long[list.size()]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Float[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Float> list = xd.getFLOATAList();
                if (!list.isEmpty()) {
                    Float[] array = list.toArray(new Float[list.size()]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Double[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<Double> list = xd.getDOUBLEAList();
                if (!list.isEmpty()) {
                    Double[] array = list.toArray(new Double[list.size()]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(String[].class)) {
                xMsgData xd = xMsgData.parseFrom(data);
                List<String> list = xd.getSTRINGAList();
                if (!list.isEmpty()) {
                    String[] array = list.toArray(new String[list.size()]);
                    return dataType.cast(array);
                }

            } else if (dataType.equals(Object.class)) {
                try {
                    return dataType.cast(xMsgUtil.deserialize(data));
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException("Could not deserialize data", e);
                }
            }

            throw new IllegalArgumentException("Invalid data type: " + dataType);

        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Message doesn't contain a valid xMsg data buffer");
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
        xMsgMessage res = createFrom(resTopic, data);
        return res;
    }
}
