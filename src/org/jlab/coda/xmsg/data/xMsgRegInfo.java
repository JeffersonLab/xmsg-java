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
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;

/**
 * Defines the parameters to register an xMsg actor with the registrar service.
 */
public final class xMsgRegInfo {

    private final xMsgRegistration.OwnerType type;
    private final xMsgTopic topic;
    private final String description;

    /**
     * Describes a publication to the given topic.
     *
     * @param topic the topic to which messages are published
     * @param description general description of the published messages
     * @return the information required to (de)register an xMsg actor as a publisher
     */
    public static xMsgRegInfo publisher(xMsgTopic topic, String description) {
        return new xMsgRegInfo(xMsgRegistration.OwnerType.PUBLISHER, topic, description);
    }

    /**
     * Describes a publication to the given topic.
     *
     * @param topic the topic to which messages are published
     * @return the information required to (de)register an xMsg actor as a publisher
     */
    public static xMsgRegInfo publisher(xMsgTopic topic) {
        return publisher(topic, "");
    }

    /**
     * Describes a subscription to the given topic.
     *
     * @param topic the topic of the subscription
     * @param description general description of the subscription
     * @return the information required to (de)register an xMsg actor as a subscriber
     */
    public static xMsgRegInfo subscriber(xMsgTopic topic, String description) {
        return new xMsgRegInfo(xMsgRegistration.OwnerType.SUBSCRIBER, topic, description);
    }

    /**
     * Describes a subscription to the given topic.
     *
     * @param topic the topic of the subscription
     * @return the information required to (de)register an xMsg actor as a subscriber
     */
    public static xMsgRegInfo subscriber(xMsgTopic topic) {
        return subscriber(topic, "");
    }

    private xMsgRegInfo(xMsgRegistration.OwnerType type, xMsgTopic topic, String description) {
        this.type = type;
        this.topic = topic;
        this.description = description;
    }

    /**
     * Returns the type parameter (publisher or subscriber).
     *
     * @return the registration type
     */
    public xMsgRegistration.OwnerType type() {
        return type;
    }

    /**
     * Returns the topic parameter.
     *
     * @return the registration topic
     */
    public xMsgTopic topic() {
        return topic;
    }

    /**
     * Returns the description parameter.
     *
     * @return the registration description
     */
    public String description() {
        return description;
    }
}
