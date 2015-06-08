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
        core.registerPublisher("asimov", "writer", "scifi", "book", "test pub");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", true);
        validator.desc = "test pub";
        validator.assertRegistration();
    }


    @Test
    public void registerLocalPublisher() throws Exception {
        core.registerLocalPublisher("asimov", "writer", "scifi", "book", "test pub");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", true);
        validator.desc = "test pub";
        validator.assertLocalRegistration();
    }


    @Test
    public void registerSubscriber() throws Exception {

        core.registerSubscriber("asimov", "writer", "scifi", "book", "test sub");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", false);
        validator.desc = "test sub";
        validator.assertRegistration();
    }


    @Test
    public void registerLocalSubscriber() throws Exception {

        core.registerLocalSubscriber("asimov", "writer", "scifi", "book", "test sub");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", false);
        validator.desc = "test sub";
        validator.assertLocalRegistration();
    }


    @Test
    public void removePublisher() throws Exception {
        core.removePublisher("asimov", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", true);
        validator.assertRemove();
    }


    @Test
    public void removeLocalPublisher() throws Exception {
        core.removeLocalPublisher("asimov", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", true);
        validator.assertLocalRemove();
    }


    @Test
    public void removeSubscriber() throws Exception {
        core.removeSubscriber("asimov", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", false);
        validator.assertRemove();
    }


    @Test
    public void removeLocalSubscriber() throws Exception {
        core.removeLocalSubscriber("asimov", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("asimov", "writer:scifi:book", false);
        validator.assertLocalRemove();
    }


    @Test
    public void findPublishersDomainOnly() throws Exception {
        core.findPublishers("sender", "writer", "*", "*");

        RegValidator validator = new RegValidator("sender", "writer:undefined:undefined", true);
        validator.assertFind();
    }


    @Test
    public void findLocalPublishersDomainOnly() throws Exception {
        core.findLocalPublishers("sender", "writer", "*", "*");

        RegValidator validator = new RegValidator("sender", "writer:undefined:undefined", true);
        validator.assertLocalFind();
    }


    @Test
    public void findPublishersDomainAndSubject() throws Exception {
        core.findPublishers("sender", "writer", "scifi", "*");

        RegValidator validator = new RegValidator("sender", "writer:scifi:undefined", true);
        validator.assertFind();
    }


    @Test
    public void findLocalPublishersDomainAndSubject() throws Exception {
        core.findLocalPublishers("sender", "writer", "scifi", "*");

        RegValidator validator = new RegValidator("sender", "writer:scifi:undefined", true);
        validator.assertLocalFind();
    }


    @Test
    public void findPublishersFullTopic() throws Exception {
        core.findPublishers("sender", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("sender", "writer:scifi:book", true);
        validator.assertFind();
    }


    @Test
    public void findLocalPublishersFullTopic() throws Exception {
        core.findLocalPublishers("sender", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("sender", "writer:scifi:book", true);
        validator.assertLocalFind();
    }


    @Test
    public void findSubscribersDomainOnly() throws Exception {
        core.findSubscribers("sender", "writer", "*", "*");

        RegValidator validator = new RegValidator("sender", "writer:undefined:undefined", false);
        validator.assertFind();
    }


    @Test
    public void findLocalSubscribersDomainOnly() throws Exception {
        core.findLocalSubscribers("sender", "writer", "*", "*");

        RegValidator validator = new RegValidator("sender", "writer:undefined:undefined", false);
        validator.assertLocalFind();
    }


    @Test
    public void findSubscribersDomainAndSubject() throws Exception {
        core.findSubscribers("sender", "writer", "scifi", "*");

        RegValidator validator = new RegValidator("sender", "writer:scifi:undefined", false);
        validator.assertFind();
    }


    @Test
    public void findLocalSubscribersDomainAndSubject() throws Exception {
        core.findLocalSubscribers("sender", "writer", "scifi", "*");

        RegValidator validator = new RegValidator("sender", "writer:scifi:undefined", false);
        validator.assertLocalFind();
    }


    @Test
    public void findSubscribersFullTopic() throws Exception {
        core.findSubscribers("sender", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("sender", "writer:scifi:book", false);
        validator.assertFind();
    }


    @Test
    public void findLocalSubscribersFullTopic() throws Exception {
        core.findLocalSubscribers("sender", "writer", "scifi", "book");

        RegValidator validator = new RegValidator("sender", "writer:scifi:book", false);
        validator.assertLocalFind();
    }


    private class RegValidator {

        private String name;
        private String host;
        private int port;
        private String topic;
        private String desc;
        private boolean isPublisher;

        private ArgumentCaptor<xMsgRegistration> dataArg;

        public RegValidator(String name, String topic, boolean isPublisher)
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
            data.setDomain(xMsgUtil.getTopicDomain(topic));
            data.setSubject(xMsgUtil.getTopicSubject(topic));
            data.setType(xMsgUtil.getTopicType(topic));
            data.setOwnerType(dataType);
            if (!desc.isEmpty()) {
                data.setDescription(desc);
            }
            return data.build();
        }
    }
}
