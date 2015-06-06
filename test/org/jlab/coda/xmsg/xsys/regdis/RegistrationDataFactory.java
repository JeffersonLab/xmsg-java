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

package org.jlab.coda.xmsg.xsys.regdis;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;

public final class RegistrationDataFactory {

    private RegistrationDataFactory() { }


    public static Builder newRegistration(String name,
                                          String host,
                                          String topic,
                                          boolean isPublisher) {
        xMsgRegistration.OwnerType dataType = isPublisher
                ? xMsgRegistration.OwnerType.PUBLISHER
                : xMsgRegistration.OwnerType.SUBSCRIBER;
        Builder data = xMsgRegistration.newBuilder();
        data.setName(name);
        data.setHost(host);
        data.setPort(xMsgConstants.DEFAULT_PORT.getIntValue());
        data.setDomain(xMsgUtil.getTopicDomain(topic));
        data.setSubject(xMsgUtil.getTopicSubject(topic));
        data.setType(xMsgUtil.getTopicType(topic));
        data.setOwnerType(dataType);
        data.setDescription(name + " test data");
        return data;
    }
}
