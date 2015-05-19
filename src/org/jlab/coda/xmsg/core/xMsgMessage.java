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
 * <p>
 *     xMsgMessage class defines a message to be serialized and sent.
 *
 *     Uses xMsgData class generated as a result of the proto-buffer
 *     description to pass Java primitive types and arrays of primitive types.
 *     xMsgData is also used to pass byte[]: the result of a user specific
 *     object serialization.
 *
 *     This class will also contain complete metadata of the message data,
 *     describing details of the data. In case this class is constructed
 *     without a metadata, the default metadata will be created and the
 *     proper data type will set based on the passed data parameter type.
 *
 *     Note that data that is an instance of byte[] will be considered
 *     to be a serialization of a specific user object only in the case
 *     when a proper
 *
 * </p>
 *
 *
 * @author gurjyan
 * @version 2.x
 * @since 11/5/14
 */
public class xMsgMessage {


    /**
     * Message address section
     */
    private String topic = xMsgConstants.UNDEFINED.getStringValue();
    private xMsgMeta.Builder metaData = null;
    private Object data = null;

    public xMsgMessage(String topic) {
        this.topic = topic;
        setData(xMsgConstants.UNDEFINED.getStringValue());
    }

    /**
     * <p>
     * This constructor will auto-create a metadata
     * object based on the type of the passed data object.
     * <p/>
     * </p>
     *
     * @param topic of the communication
     * @param data  of the communication
     */
    public xMsgMessage(String topic,
                       Object data) {
        this.topic = topic;
        setData(data);
    }

    public xMsgMessage( String topic,
                        xMsgMeta.Builder metaData,
                        Object data) {
        this.topic = topic;
        this.metaData = metaData;
        this.data = data;
    }

    public boolean getIsDataSerialized() {
        if (metaData != null) return metaData.getIsDataSerialized();
        else {
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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
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
     * <p>
     * This method will check to see if passed objects are of
     * primitive types or an array of primitive types, and will assume
     * transferring them (serializing) through xMsgData object.
     * Any other Java object will be considered to be passed as
     * un-serialized J_Object.
     * Note. if you pass to this method a byte[] as a result of your
     * own serialization process it will be set as the xMsgData byte[]
     * with the type T_BYTES. In this case your actual data type will be
     * lost. This is the mechanism to pass your own serialized byte[]
     * through xMsgData (type = X_Object).
     * </p>
     *
     * @param data object
     */
    public void setData(Object data) {
        if (metaData == null) {
            metaData = xMsgMeta.newBuilder();
        }

        // xMsgData object for all primitive types
        xMsgData.Builder d = xMsgData.newBuilder();
        metaData.setDataType(xMsgMeta.DataType.X_Object);
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

            // Any Java object (un serialized)
        } else {
            metaData.setDataType(xMsgMeta.DataType.J_Object);
            metaData.setIsDataSerialized(false);
            this.data = data;
        }
    }

    /**
     * <p>
     * This method sets the data object using specified data type.
     * </p>
     *
     * @param data object
     * @param type of the data object
     */
    public void setData(Object data, xMsgMeta.DataType type) {
        if (metaData == null) {
            metaData = xMsgMeta.newBuilder();
        }

        // data type defined is xMsgData
        if (type.equals(xMsgMeta.DataType.X_Object)) {
            if (data instanceof xMsgData) {
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                metaData.setIsDataSerialized(true);
                this.data = data;

                // un-serialized xMsgData object (X_Object)
            } else if (data instanceof xMsgData.Builder) {
                metaData.setDataType(xMsgMeta.DataType.X_Object);
                metaData.setIsDataSerialized(false);
                this.data = data;
            }
        } else {

            // This is the user defined data type, such as
            // NETCDFS_Object, P_Object, C_Object, etc.
            metaData.setDataType(type);

            // If user object has type =  byte[], then it
            // is considered to be a serialized object.
            if (data instanceof byte[]) {
                metaData.setIsDataSerialized(true);
            } else {
                metaData.setIsDataSerialized(false);
            }
            this.data = data;
        }
    }

    public void setData(Object data, xMsgMeta.Builder metadata) {
        this.data = data;
        this.metaData = metadata;
    }

}
