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

package org.jlab.coda.xmsg.data;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegFactory;

/**
 * Defines the parameters to search actors in the registrar service.
 */
public final class xMsgRegQuery {

    private final xMsgRegistration.Builder data;
    private final Category category;

    /**
     * Creates a simple query to search publishers of the specified topic.
     *
     * @param topic the topic of interest
     * @return a query object
     */
    public static xMsgRegQuery publishers(xMsgTopic topic) {
        return publishers().matching(topic);
    }

    /**
     * Creates a factory of queries to search publishers.
     *
     * @return a queries factory
     */
    public static Factory publishers() {
        return new Factory(xMsgRegistration.OwnerType.PUBLISHER);
    }

    /**
     * Creates a simple query to search subscribers of the specified topic.
     *
     * @param topic the topic of interest
     * @return a query object
     */
    public static xMsgRegQuery subscribers(xMsgTopic topic) {
        return subscribers().matching(topic);
    }

    /**
     * Creates a factory of queries to search subscribers.
     *
     * @return a queries factory
     */
    public static Factory subscribers() {
        return new Factory(xMsgRegistration.OwnerType.SUBSCRIBER);
    }


    /**
     * A classification of registration queries.
     * Each category uses a different driver method.
     */
    public enum Category {
        /** Query the registrar by topic matching. */
        MATCHING,

        /** Query the registrar by comparing registration data. */
        FILTER,

        /** Query the registrar by exact topic. */
        EXACT,

        /** Query the registrar for all actors. */
        ALL
    }


    /**
     * Creates specific registration discovery queries.
     */
    public static final class Factory {

        private final xMsgRegistration.Builder data;

        private Factory(xMsgRegistration.OwnerType type) {
            data = xMsgRegFactory.newFilter(type);
        }

        /**
         * A query for registered actors matching the given topic.
         *
         * @param topic the topic to be matched
         * @return a query for actors with topics matching the given topic
         */
        public xMsgRegQuery matching(xMsgTopic topic) {
            data.setDomain(topic.domain());
            data.setSubject(topic.subject());
            data.setType(topic.type());
            return new xMsgRegQuery(data, Category.MATCHING);
        }

        /**
         * A query for registered actors with this exact domain.
         *
         * @param domain the expected domain
         * @return a query for actors registered to the given domain
         *         (subject and type are ignored)
         */
        public xMsgRegQuery withDomain(String domain) {
            data.setDomain(domain);
            return new xMsgRegQuery(data, Category.FILTER);
        }

        /**
         * A query for registered actors with this exact subject.
         *
         * @param subject the expected subject
         * @return a query for actors registered to the given subject
         *         (domain and type are ignored)
         */
        public xMsgRegQuery withSubject(String subject) {
            data.setSubject(subject);
            return new xMsgRegQuery(data, Category.FILTER);
        }

        /**
         * A query for registered actor with this exact type.
         *
         * @param type the expected type
         * @return a query for actors registered to the given type
         *         (domain and subject are ignored)
         */
        public xMsgRegQuery withType(String type) {
            data.setType(type);
            return new xMsgRegQuery(data, Category.FILTER);
        }

        /**
         * A query for registered actors with this exact topic.
         *
         * @param topic the topic to be compared
         * @return a query for actors with the same topic as the given topic
         */
        public xMsgRegQuery withSame(xMsgTopic topic) {
            data.setDomain(topic.domain());
            data.setSubject(topic.subject());
            data.setType(topic.type());
            return new xMsgRegQuery(data, Category.EXACT);
        }

        /**
         * A query for registered actors with this exact hostname.
         *
         * @param host the expected host
         * @return a query for actors registered from the given host
         */
        public xMsgRegQuery withHost(String host) {
            data.setHost(xMsgUtil.toHostAddress(host));
            return new xMsgRegQuery(data, Category.FILTER);
        }

        /**
         * A query to get all registered actors.
         *
         * @return a query for all registered actors
         */
        public xMsgRegQuery all() {
            return new xMsgRegQuery(data, Category.ALL);
        }
    }


    private xMsgRegQuery(xMsgRegistration.Builder data, Category category) {
        this.data = data;
        this.category = category;
    }

    /**
     * Serializes the query into a protobuf object.
     *
     * @return the query as a registration data object
     */
    public xMsgRegistration.Builder data() {
        return data;
    }

    /**
     * The category of the query.
     *
     * @return the category
     */
    public Category category() {
        return category;
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
