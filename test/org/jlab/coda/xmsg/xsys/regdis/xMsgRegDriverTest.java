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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.net.xMsgSocketFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory.newFilter;
import static org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory.newRegistration;

import static org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType.PUBLISHER;
import static org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType.SUBSCRIBER;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class xMsgRegDriverTest {

    private xMsgRegDriver driver;
    private String sender = "testSender";

    private final String topic = "writer:scifi:books";


    @Before
    public void setup() throws Exception {
        xMsgRegAddress address = new xMsgRegAddress("10.2.9.1");
        xMsgSocketFactory factory = mock(xMsgSocketFactory.class);
        driver = spy(new xMsgRegDriver(address, factory));
        setResponse(new xMsgRegResponse("", ""));
    }


    @Test
    public void sendPublisherRegistration() throws Exception {
        Builder publisher = newRegistration("bradbury_pub", PUBLISHER, topic);
        publisher.setDescription("bradbury books");

        driver.addRegistration(sender, publisher.build());

        assertRequest(publisher.build(),
                      xMsgConstants.REGISTER_PUBLISHER,
                      xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberRegistration() throws Exception {
        Builder subscriber = newRegistration("bradbury_sub", SUBSCRIBER, topic);
        subscriber.setDescription("bradbury books");

        driver.addRegistration(sender, subscriber.build());

        assertRequest(subscriber.build(),
                      xMsgConstants.REGISTER_SUBSCRIBER,
                      xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendPublisherRemoval() throws Exception {
        Builder publisher = newRegistration("bradbury_pub", PUBLISHER, topic);

        driver.removeRegistration(sender, publisher.build());

        assertRequest(publisher.build(),
                      xMsgConstants.REMOVE_PUBLISHER,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberRemoval() throws Exception {
        Builder subscriber = newRegistration("bradbury_sub", SUBSCRIBER, topic);

        driver.removeRegistration(sender, subscriber.build());

        assertRequest(subscriber.build(),
                      xMsgConstants.REMOVE_SUBSCRIBER,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendHostRemoval() throws Exception {
        driver.removeAllRegistration(sender, "10.2.9.1");

        assertRequest("10.2.9.1",
                      xMsgConstants.REMOVE_ALL_REGISTRATION,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendPublisherFind() throws Exception {
        Builder data = newRegistration("", PUBLISHER, topic);

        driver.findRegistration(sender, data.build());

        assertRequest(data.build(),
                      xMsgConstants.FIND_PUBLISHER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberFind() throws Exception {
        Builder data = newRegistration("", SUBSCRIBER, topic);

        driver.findRegistration(sender, data.build());

        assertRequest(data.build(),
                      xMsgConstants.FIND_SUBSCRIBER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void sendPublisherFilter() throws Exception {
        Builder data = newFilter(PUBLISHER);
        data.setDomain("domain");

        driver.filterRegistration(sender, data.build());

        assertRequest(data.build(),
                      xMsgConstants.FILTER_PUBLISHER,
                      xMsgConstants.FILTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberFilter() throws Exception {
        Builder data = newFilter(SUBSCRIBER);
        data.setDomain("domain");

        driver.filterRegistration(sender, data.build());

        assertRequest(data.build(),
                      xMsgConstants.FILTER_SUBSCRIBER,
                      xMsgConstants.FILTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendPublisherAll() throws Exception {
        Builder data = newFilter(PUBLISHER);

        driver.allRegistration(sender, data.build());

        assertRequest(data.build(),
                      xMsgConstants.ALL_PUBLISHER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void sendSubscriberAll() throws Exception {
        Builder data = newFilter(SUBSCRIBER);

        driver.allRegistration(sender, data.build());

        assertRequest(data.build(),
                      xMsgConstants.ALL_SUBSCRIBER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void getRegistration() throws Exception {
        Builder data = newRegistration("", PUBLISHER, topic);

        Builder pub1 = newRegistration("bradbury1", PUBLISHER, topic);
        Builder pub2 = newRegistration("bradbury2", PUBLISHER, topic);
        Set<xMsgRegistration> regData = new HashSet<>(Arrays.asList(pub1.build(), pub2.build()));

        setResponse(new xMsgRegResponse("", "", regData));

        Set<xMsgRegistration> regRes = driver.findRegistration(sender, data.build());

        assertThat(regRes, is(regData));
    }



    private void assertRequest(xMsgRegistration data, String topic, int timeout)
            throws Exception {
        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        verify(driver).request(request, timeout);
    }


    private void assertRequest(String data, String topic, int timeout)
            throws Exception {
        xMsgRegRequest request = new xMsgRegRequest(topic, sender, data);
        verify(driver).request(request, timeout);
    }


    private void setResponse(xMsgRegResponse response) throws Exception {
        doReturn(response).when(driver).request(any(xMsgRegRequest.class), anyInt());
    }
}
