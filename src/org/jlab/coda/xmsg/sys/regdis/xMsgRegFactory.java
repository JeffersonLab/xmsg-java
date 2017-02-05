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

package org.jlab.coda.xmsg.sys.regdis;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

/**
 * Ensures the egistration requests are properly constructed.
 */
public final class xMsgRegFactory {

    private xMsgRegFactory() { }

    /**
     * Creates the data for a registration request.
     *
     * @param name the name of the actor
     * @param host the host of the actor
     * @param type the type of the actor (publisher or subscriber)
     * @param topic the topic to be registered
     * @return the registration data
     */
    public static xMsgRegistration.Builder newRegistration(String name,
                                                           String host,
                                                           xMsgRegistration.OwnerType type,
                                                           xMsgTopic topic) {
        return newRegistration(name, new xMsgProxyAddress(host), type, topic);
    }

    /**
     * Creates the data for a registration request.
     *
     * @param name the name of the actor
     * @param address the address of the actor
     * @param type the type of the actor (publisher or subscriber)
     * @param topic the topic to be registered
     * @return the registration data
     */
    public static xMsgRegistration.Builder newRegistration(String name,
                                                           xMsgProxyAddress address,
                                                           xMsgRegistration.OwnerType type,
                                                           xMsgTopic topic) {
        xMsgRegistration.Builder regb = xMsgRegistration.newBuilder();
        regb.setName(name);
        regb.setHost(address.host());
        regb.setPort(address.pubPort());
        regb.setDomain(topic.domain());
        regb.setSubject(topic.subject());
        regb.setType(topic.type());
        regb.setOwnerType(type);
        return regb;
    }

    /**
     * Creates an empty filter request.
     * The terms to filter the actors need to be set.
     *
     * @param type the type of the actor (publisher or subscriber)
     * @return the registration data to filter actors
     */
    public static xMsgRegistration.Builder newFilter(xMsgRegistration.OwnerType type) {
        xMsgRegistration.Builder filter = xMsgRegistration.newBuilder();
        filter.setName(xMsgRegConstants.UNDEFINED);
        filter.setHost(xMsgRegConstants.UNDEFINED);
        filter.setPort(0);
        filter.setDomain(xMsgConstants.ANY);
        filter.setSubject(xMsgConstants.ANY);
        filter.setType(xMsgConstants.ANY);
        filter.setOwnerType(type);
        return filter;
    }
}
