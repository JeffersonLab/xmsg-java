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

import org.jlab.coda.xmsg.excp.xMsgException;

/**
 * <p>
 *     xMsgMessage class defines a message to be serialized and sent.
 *     Uses xMsgData class generated as a result of the proto-buffer
 *     description.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 11/5/14
 */
public class xMsgMessage {

    /**
     * Message address section
     */
    private String topic = xMsgConstants.UNDEFINED.getStringValue();
    private String dataType = xMsgConstants.UNDEFINED.getStringValue();
    private String domain = xMsgConstants.UNDEFINED.getStringValue();
    private String subject = xMsgConstants.UNDEFINED.getStringValue();
    private String type = xMsgConstants.UNDEFINED.getStringValue();
    private Boolean isSyncRequest = false;
    private String syncRequesterAddress = xMsgConstants.UNDEFINED.getStringValue();
    /**
     * Message data section
     */
    private Object data;

    public xMsgMessage(){

    }

    public xMsgMessage( String topic,
                        String dataType,
                       Object data) throws xMsgException {
        this.topic = topic;
        this.dataType = dataType;
        this.data = data;
    }


    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dt) {
        this.dataType = dt;
    }

    public String getDomain() throws xMsgException {
        if(domain.equals(xMsgConstants.UNDEFINED.getStringValue())){
            domain = xMsgUtil.getTopicDomain(topic);
        }
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getSubject() throws xMsgException {
        if(subject.equals(xMsgConstants.UNDEFINED.getStringValue())){
            subject = xMsgUtil.getTopicSubject(topic);
        }
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getType() throws xMsgException {
        if(type.equals(xMsgConstants.UNDEFINED.getStringValue())){
            type = xMsgUtil.getTopicType(topic);
        }
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Boolean getIsSyncRequest() {
        return isSyncRequest;
    }

    public void setIsSyncRequest(Boolean isSyncRequest) {
        this.isSyncRequest = isSyncRequest;
    }

    public String getSyncRequesterAddress() {
        return syncRequesterAddress;
    }

    public void setSyncRequesterAddress(String syncRequesterAddress) {
        this.syncRequesterAddress = syncRequesterAddress;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "xMsgMessage{" +
                "dataType='" + dataType + '\'' +
                ", domain='" + domain + '\'' +
                ", subject='" + subject + '\'' +
                ", type='" + type + '\'' +
                ", isSyncRequest=" + isSyncRequest +
                ", syncRequesterAddress='" + syncRequesterAddress + '\'' +
                ", data=" + data +
                '}';
    }
}
