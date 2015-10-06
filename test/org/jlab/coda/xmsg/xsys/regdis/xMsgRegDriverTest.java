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
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class xMsgRegDriverTest {

    private xMsgRegDriver driver;

    private Builder subscriber;
    private Builder publisher;
    private Set<xMsgRegistration> registration;


    public xMsgRegDriverTest() {
        publisher = xMsgRegistration.newBuilder();
        publisher.setName("bradbury_pub");
        publisher.setHost("localhost");
        publisher.setPort(xMsgConstants.DEFAULT_PORT.getIntValue());
        publisher.setDomain("writer");
        publisher.setSubject("scifi");
        publisher.setType("books");
        publisher.setOwnerType(xMsgRegistration.OwnerType.PUBLISHER);
        publisher.setDescription("bradbury books");

        subscriber = xMsgRegistration.newBuilder();
        subscriber.setName("bradbury_sub");
        subscriber.setHost("localhost");
        subscriber.setPort(xMsgConstants.DEFAULT_PORT.getIntValue());
        subscriber.setDomain("writer");
        subscriber.setSubject("scifi");
        subscriber.setType("books");
        subscriber.setOwnerType(xMsgRegistration.OwnerType.SUBSCRIBER);
        subscriber.setDescription("bradbury books");

        registration = new HashSet<>(Arrays.asList(publisher.build(), subscriber.build()));
    }


    @Before
    public void setup() throws Exception {
        driver = spy(new xMsgRegDriver(new ZContext(), "10.2.9.1"));
        setResponse(new xMsgRegResponse("", ""));
    }


    @Test
    public void sendPublisherRegistration() throws Exception {
        driver.register(publisher.build(), true);
        assertRequest("bradbury_pub",
                publisher.build(),
                xMsgConstants.REGISTER_PUBLISHER,
                xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberRegistration() throws Exception {
        driver.register(subscriber.build(), false);
        assertRequest("bradbury_sub",
                subscriber.build(),
                xMsgConstants.REGISTER_SUBSCRIBER,
                xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendPublisherRemoval() throws Exception {
        driver.removeRegistration(publisher.build(), true);
        assertRequest("bradbury_pub",
                publisher.build(),
                xMsgConstants.REMOVE_PUBLISHER,
                xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberRemoval() throws Exception {
        driver.removeRegistration(subscriber.build(), false);
        assertRequest("bradbury_sub",
                subscriber.build(),
                xMsgConstants.REMOVE_SUBSCRIBER,
                xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendHostRemoval() throws Exception {
        driver.removeAll();

        assertRequest("10.2.9.1_node",
                "10.2.9.1",
                xMsgConstants.REMOVE_ALL_REGISTRATION,
                xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendPublisherFind() throws Exception {
        driver.findRegistration(publisher.build(), false);

        assertRequest("10.2.9.1_node",
                publisher.build(),
                xMsgConstants.FIND_PUBLISHER,
                xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberFind() throws Exception {
        driver.findRegistration(subscriber.build(), false);

        assertRequest("10.2.9.1_node",
                subscriber.build(),
                xMsgConstants.FIND_SUBSCRIBER,
                xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void getRegistration() throws Exception {
        setResponse(new xMsgRegResponse("", "", registration));
        Set<xMsgRegistration> res = driver.findRegistration(subscriber.build(), false);
        assertThat(res, is(registration));
    }



    private void assertRequest(String name, xMsgRegistration data,
                               xMsgConstants topic, xMsgConstants timeout)
            throws Exception {
        xMsgRegRequest request = new xMsgRegRequest(topic.getStringValue(), name, data);
        verify(driver).request(request, timeout.getIntValue());
    }


    private void assertRequest(String name, String data,
                               xMsgConstants topic, xMsgConstants timeout)
            throws Exception {
        xMsgRegRequest request = new xMsgRegRequest(topic.getStringValue(), name, data);
        verify(driver).request(request, timeout.getIntValue());
    }


    private void setResponse(xMsgRegResponse response) throws Exception {
        doReturn(response).when(driver).request(any(xMsgRegRequest.class), anyInt());
    }
}
