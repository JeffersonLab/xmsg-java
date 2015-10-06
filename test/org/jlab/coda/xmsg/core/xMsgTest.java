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

import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.zeromq.ZContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class xMsgTest {

    private xMsgRegDriver driver;
    private xMsg core;
    private String name = "asimov";

    @Before
    public void setup() throws Exception {
        driver = mock(xMsgRegDriver.class);
        when(driver.getContext()).thenReturn(mock(ZContext.class));
        when(driver.getAddress()).thenReturn(xMsgUtil.localhost());
        core = new xMsg(name);
    }


    @Test
    public void registerPublisher() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.registerAsPublisher(topic, "test pub");

        RegValidator validator = new RegValidator(topic, true);
        validator.desc = "test pub";
        validator.assertDriverCall("register");
    }

    @Test
    public void registerSubscriber() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.registerAsSubscriber(topic, "test sub");

        RegValidator validator = new RegValidator(topic, false);
        validator.desc = "test sub";
        validator.assertDriverCall("register");
    }

    @Test
    public void removePublisher() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.removePublisherRegistration(topic);

        RegValidator validator = new RegValidator(topic, true);
        validator.assertDriverCall("remove");
    }

    @Test
    public void removeSubscriber() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.removeSubscriberRegistration(topic);

        RegValidator validator = new RegValidator(topic, false);
        validator.assertDriverCall("remove");
    }



    @Test
    public void findPublishers() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.findPublishers(topic);

        RegValidator validator = new RegValidator(topic, true);
        validator.assertDriverCall("find");
    }


    @Test
    public void findSubscribers() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi");
        core.findSubscribers(topic);

        RegValidator validator = new RegValidator(topic, false);
        validator.assertDriverCall("find");
    }



    private class RegValidator {

        private String host;
        private int port;
        private xMsgTopic topic;
        private String desc;
        private boolean isPublisher;

        private ArgumentCaptor<xMsgRegistration> dataArg;

        public RegValidator(xMsgTopic topic, boolean isPublisher)
                throws Exception {
            this.host = xMsgUtil.toHostAddress("localhost");
            this.port = xMsgConstants.DEFAULT_PORT.getIntValue();
            this.topic = topic;
            this.desc = "";

            this.isPublisher = isPublisher;
            this.dataArg = ArgumentCaptor.forClass(xMsgRegistration.class);
        }

        private void assertDriverCall(String method) throws Exception {
            verifyCalled(method);
            verifyData();
        }

        private void verifyCalled(String method) throws Exception {
            xMsgRegDriver v = verify(driver);
            switch (method) {
                case "register":
                    v.register(dataArg.capture(), eq(isPublisher));
                    break;
                case "remove":
                    v.removeRegistration(dataArg.capture(), eq(isPublisher));
                    break;
                case "find":
                    v.findRegistration(dataArg.capture(), eq(isPublisher));
                    break;
                default:
                    throw new IllegalArgumentException("Illegal method " + method);
            }
        }

        private void verifyData() throws Exception {
            xMsgRegistration data = dataArg.getValue();
            assertThat(data, is(expectedData()));
        }

        private xMsgRegistration expectedData()
                throws Exception {
            xMsgRegistration.OwnerType dataType = isPublisher
                    ? xMsgRegistration.OwnerType.PUBLISHER
                    : xMsgRegistration.OwnerType.SUBSCRIBER;
            Builder data = xMsgRegistration.newBuilder();
            data.setName(name);
            data.setHost(host);
            data.setPort(port);
            data.setDomain(topic.domain());
            data.setSubject(topic.subject());
            data.setType(topic.type());
            data.setOwnerType(dataType);
            if (!desc.isEmpty()) {
                data.setDescription(desc);
            }
            return data.build();
        }
    }
}
