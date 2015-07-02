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

import com.google.protobuf.ByteString;

import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMeta;

import java.util.Arrays;

/**
 * Defines a message to be serialized and passed through 0MQ.
 *
 * Uses {@link xMsgData} class generated as a result of the proto-buffer
 * description to pass Java primitive types and arrays of primitive types.
 * xMsgData is also used to pass byte[]: the result of a user specific
 * object serialization.
 * <p>
 * This class will also contain complete metadata of the message data,
 * describing details of the data. In case an object is constructed
 * without a metadata, the default metadata will be created and the
 * proper data type will set based on the passed data parameter type.
 * <p>
 * Note that data that is an instance of {@code byte[]} will be considered to be
 * a serialization of a specific user object only in the case when a proper
 *
 * @author gurjyan
 * @version 2.x
 * @since 11/5/14
 */
public class xMsgMessage {

    private xMsgTopic topic;
    private xMsgMeta.Builder metaData = null;
    private Object data = null;

    public xMsgMessage(xMsgTopic topic) {
        this.topic = topic;
        setData(xMsgConstants.UNDEFINED.getStringValue(),"native");
    }

    /**
     * Constructor that will auto-create the metadata.
     * This metadata will be based on the type of the passed data object.
     *
     * @param topic of the communication
     * @param data  of the communication
     */
    public xMsgMessage(xMsgTopic topic,
                       Object data, String dataType) {
        this.topic = topic;
        setData(data, dataType);
    }

    /**
     * Constructor.
     *
     * @param topic of the communication
     * @param metadata of the communication
     * @param data of the communication
     */
    public xMsgMessage(xMsgTopic topic,
                       xMsgMeta.Builder metadata,
                       Object data) {
        this.topic = topic;
        this.metaData = metadata;
        this.data = data;
    }

    public boolean getIsDataSerialized() {
        if (metaData != null) {
            return metaData.getIsDataSerialized();
        } else {
            metaData = xMsgMeta.newBuilder();
            return metaData.getIsDataSerialized();
        }
    }

    public void setIsDataSerialized(boolean b) {
        if (metaData == null) {
            metaData = xMsgMeta.newBuilder();
            metaData.setIsDataSerialized(b);
        } else {
            metaData.setIsDataSerialized(b);
        }
    }

    public xMsgTopic getTopic() {
        return topic;
    }

    public void setTopic(xMsgTopic topic) {
        this.topic = topic;
    }

    public xMsgMeta.Builder getMetaData() {
        return metaData;
    }

    public void setMetaData(xMsgMeta.Builder metaData) {
        this.metaData = metaData;
    }

    public Object getData() {
        return data;
    }

    /**
     * Sets the message data.
     * <p>
     * This method will check to see if passed objects are of
     * primitive types or an array of primitive types, and will assume
     * transferring them (serializing) through xMsgData object.
     * Any other Java object will be considered to be passed as
     * un-serialized J_Object.
     * <p>
     * Note. if you pass to this method a byte[] as a result of your
     * own serialization process it will be set as the xMsgData byte[]
     * with the type T_BYTES. In this case your actual data type will be
     * lost. This is the mechanism to pass your own serialized byte[]
     * through xMsgData (type = X_Object).
     *
     * @param data object
     */
    public void setData(Object data, String dataType) {
        if (metaData == null) {
            metaData = xMsgMeta.newBuilder();
        }

        // xMsgData object for all primitive types
        xMsgData.Builder d = xMsgData.newBuilder();
        metaData.setDataType("native");
        if (data instanceof String) {
            d.setSTRING((String) data);
            d.setType(xMsgData.Type.T_STRING);
            this.data = d;
        } else if (data instanceof Integer) {
            d.setFLSINT32((Integer) data);
            d.setType(xMsgData.Type.T_FLSINT32);
            this.data = d;
        } else if (data instanceof Long) {
            d.setFLSINT64((Long) data);
            d.setType(xMsgData.Type.T_FLSINT64);
            this.data = d;
        } else if (data instanceof Float) {
            d.setFLOAT((Float) data);
            d.setType(xMsgData.Type.T_FLOAT);
            this.data = d;
        } else if (data instanceof Double) {
            d.setDOUBLE((Double) data);
            d.setType(xMsgData.Type.T_DOUBLE);
            this.data = d;

        } else if (data instanceof Integer[]) {
            Integer[] a = (Integer[]) data;
            d.addAllFLSINT32A(Arrays.asList(a));
            d.setType(xMsgData.Type.T_FLSINT32A);
            this.data = d;

        } else if (data instanceof Long[]) {
            Long[] a = (Long[]) data;
            d.addAllFLSINT64A(Arrays.asList(a));
            d.setType(xMsgData.Type.T_FLSINT64A);
            this.data = d;

        } else if (data instanceof Float[]) {
            Float[] a = (Float[]) data;
            d.addAllFLOATA(Arrays.asList(a));
            d.setType(xMsgData.Type.T_FLOATA);
            this.data = d;

        } else if (data instanceof Double[]) {
            Double[] a = (Double[]) data;
            d.addAllDOUBLEA(Arrays.asList(a));
            d.setType(xMsgData.Type.T_DOUBLEA);
            this.data = d;

        } else if (data instanceof byte[]) {
            d.setBYTES(ByteString.copyFrom((byte[]) data));
            d.setType(xMsgData.Type.T_BYTES);
            this.data = d;

            // serialized xMsgData (X_Object)
        } else if (data instanceof xMsgData) {
            metaData.setIsDataSerialized(true);
            this.data = data;

            // un-serialized xMsgData object (X_Object)
        } else if (data instanceof xMsgData.Builder) {
            metaData.setIsDataSerialized(false);
            this.data = data;

            // Any custom object (un serialized)
        } else {
            metaData.setDataType(dataType);
            metaData.setIsDataSerialized(false);
            this.data = data;
        }
    }

    /**
     * Sets the message data.
     */
    public void setData(Object data, xMsgMeta.Builder metadata) {
        this.data = data;
        this.metaData = metadata;
    }
}
