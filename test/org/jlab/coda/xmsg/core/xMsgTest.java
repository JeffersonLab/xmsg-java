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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class xMsgTest {

    private xMsgRegDriver driver;
    private xMsg core;

    @Before
    public void setup() throws Exception {
        driver = mock(xMsgRegDriver.class);
        core = new xMsg(driver, 2);
    }


    @Test
    public void registerPublisher() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.registerPublisher("asimov", topic, "test pub");

        RegValidator validator = new RegValidator("asimov", topic, true);
        validator.desc = "test pub";
        validator.assertRegistration();
    }


    @Test
    public void registerLocalPublisher() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.registerLocalPublisher("asimov", topic, "test pub");

        RegValidator validator = new RegValidator("asimov", topic, true);
        validator.desc = "test pub";
        validator.assertLocalRegistration();
    }


    @Test
    public void registerSubscriber() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.registerSubscriber("asimov", topic, "test sub");

        RegValidator validator = new RegValidator("asimov", topic, false);
        validator.desc = "test sub";
        validator.assertRegistration();
    }


    @Test
    public void registerLocalSubscriber() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.registerLocalSubscriber("asimov", topic, "test sub");

        RegValidator validator = new RegValidator("asimov", topic, false);
        validator.desc = "test sub";
        validator.assertLocalRegistration();
    }


    @Test
    public void removePublisher() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.removePublisher("asimov", topic);

        RegValidator validator = new RegValidator("asimov", topic, true);
        validator.assertRemove();
    }


    @Test
    public void removeLocalPublisher() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.removeLocalPublisher("asimov", topic);

        RegValidator validator = new RegValidator("asimov", topic, true);
        validator.assertLocalRemove();
    }


    @Test
    public void removeSubscriber() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.removeSubscriber("asimov", topic);

        RegValidator validator = new RegValidator("asimov", topic, false);
        validator.assertRemove();
    }


    @Test
    public void removeLocalSubscriber() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.removeLocalSubscriber("asimov", topic);

        RegValidator validator = new RegValidator("asimov", topic, false);
        validator.assertLocalRemove();
    }


    @Test
    public void findPublishers() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.findPublishers("sender", topic);

        RegValidator validator = new RegValidator("sender", topic, true);
        validator.assertFind();
    }


    @Test
    public void findLocalPublishers() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");

        core.findLocalPublishers("sender", topic);

        RegValidator validator = new RegValidator("sender", topic, true);
        validator.assertLocalFind();
    }


    @Test
    public void findSubscribers() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi");
        core.findSubscribers("sender", topic);

        RegValidator validator = new RegValidator("sender", topic, false);
        validator.assertFind();
    }


    @Test
    public void findLocalSubscribers() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");
        core.findLocalSubscribers("sender", topic);

        RegValidator validator = new RegValidator("sender", topic, false);
        validator.assertLocalFind();
    }


    private class RegValidator {

        private String name;
        private String host;
        private int port;
        private xMsgTopic topic;
        private String desc;
        private boolean isPublisher;

        private ArgumentCaptor<xMsgRegistration> dataArg;

        public RegValidator(String name, xMsgTopic topic, boolean isPublisher)
                throws Exception {
            this.name = name;
            this.host = xMsgUtil.toHostAddress("localhost");
            this.port = xMsgConstants.DEFAULT_PORT.getIntValue();
            this.topic = topic;
            this.desc = "";

            this.isPublisher = isPublisher;
            this.dataArg = ArgumentCaptor.forClass(xMsgRegistration.class);
        }

        public void assertRegistration() throws Exception {
            verify(driver).registerFrontEnd(eq(name), dataArg.capture(), eq(isPublisher));
            verifyData();
        }

        public void assertLocalRegistration() throws Exception {
            verify(driver).registerLocal(eq(name), dataArg.capture(), eq(isPublisher));
            verifyData();
        }

        public void assertRemove() throws Exception {
            verify(driver).removeRegistrationFrontEnd(eq(name), dataArg.capture(), eq(isPublisher));
            verifyData();
        }

        public void assertLocalRemove() throws Exception {
            verify(driver).removeRegistrationLocal(eq(name), dataArg.capture(), eq(isPublisher));
            verifyData();
        }

        public void assertFind() throws Exception {
            verify(driver).findGlobal(eq(name), dataArg.capture(), eq(isPublisher));
            verifyData();
        }

        public void assertLocalFind() throws Exception {
            verify(driver).findLocal(eq(name), dataArg.capture(), eq(isPublisher));
            verifyData();
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
