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

package org.jlab.coda.xmsg.xsys.regdis;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;

/**
 * Defines the parameters to search actors in the registrar service.
 */
public final class xMsgRegQuery {

    private final xMsgRegistration.Builder data;

    /**
     * Creates a simple query to search publishers of the specified topic.
     *
     * @param topic the topic of interest
     * @return a query object
     */
    public static xMsgRegQuery publishers(xMsgTopic topic) {
        return new xMsgRegQuery(xMsgRegistration.OwnerType.PUBLISHER, topic);
    }

    /**
     * Creates a simple query to search subscribers of the specified topic.
     *
     * @param topic the topic of interest
     * @return a query object
     */
    public static xMsgRegQuery subscribers(xMsgTopic topic) {
        return new xMsgRegQuery(xMsgRegistration.OwnerType.SUBSCRIBER, topic);
    }

    private xMsgRegQuery(xMsgRegistration.OwnerType type, xMsgTopic topic) {
        data = xMsgRegFactory.newFilter(type);
        data.setDomain(topic.domain());
        data.setSubject(topic.subject());
        data.setType(topic.type());
    }

    /**
     * Serializes the query into a protobuf object.
     */
    public xMsgRegistration.Builder data() {
        return data;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + data.hashCode();
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
        xMsgRegQuery other = (xMsgRegQuery) obj;
        return data.equals(other.data);
    }
}
