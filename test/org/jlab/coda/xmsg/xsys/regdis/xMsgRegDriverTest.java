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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class xMsgRegDriverTest {

    private xMsgRegDriver driver;

    private ZContext context;
    private Socket localCon;
    private Socket feCon;

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
        context = spy(new ZContext());
        localCon = context.createSocket(ZMQ.REQ);
        feCon = context.createSocket(ZMQ.REQ);
        when(context.createSocket(anyInt())).thenReturn(feCon, localCon);

        driver = spy(new xMsgRegDriver(context, "10.2.9.100"));
        setResponse(new xMsgRegResponse("", ""));
    }


    @Test
    public void sendLocalPublisherRegistration() throws Exception {
        driver.registerLocal("bradbury_pub", publisher.build(), true);
        assertRequest("bradbury_pub",
                      publisher.build(),
                      localCon,
                      xMsgConstants.REGISTER_PUBLISHER,
                      xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendLocalSubscriberRegistration() throws Exception {
        driver.registerLocal("bradbury_sub", subscriber.build(), false);
        assertRequest("bradbury_sub",
                      subscriber.build(),
                      localCon,
                      xMsgConstants.REGISTER_SUBSCRIBER,
                      xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendFrontEndPublisherRegistration() throws Exception {

        driver.registerFrontEnd("bradbury_pub", publisher.build(), true);

        assertRequest("bradbury_pub",
                      publisher.build(),
                      feCon,
                      xMsgConstants.REGISTER_PUBLISHER,
                      xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }


    @Test
    public void sendFrontEndSubscriberRegistration() throws Exception {

        driver.registerFrontEnd("bradbury_sub", subscriber.build(), false);

        assertRequest("bradbury_sub",
                      subscriber.build(),
                      feCon,
                      xMsgConstants.REGISTER_SUBSCRIBER,
                      xMsgConstants.REGISTER_REQUEST_TIMEOUT);
    }



    @Test
    public void sendLocalPublisherRemoval() throws Exception {
        driver.removeRegistrationLocal("bradbury_pub", publisher.build(), true);
        assertRequest("bradbury_pub",
                      publisher.build(),
                      localCon,
                      xMsgConstants.REMOVE_PUBLISHER,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendLocalSubscriberRemoval() throws Exception {
        driver.removeRegistrationLocal("bradbury_sub", subscriber.build(), false);
        assertRequest("bradbury_sub",
                      subscriber.build(),
                      localCon,
                      xMsgConstants.REMOVE_SUBSCRIBER,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendFrontEndPublisherRemoval() throws Exception {
        driver.removeRegistrationFrontEnd("bradbury_pub", publisher.build(), true);
        assertRequest("bradbury_pub",
                      publisher.build(),
                      feCon,
                      xMsgConstants.REMOVE_PUBLISHER,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendFrontEndSubscriberRemoval() throws Exception {

        driver.removeRegistrationFrontEnd("bradbury_sub", subscriber.build(), false);

        assertRequest("bradbury_sub",
                      subscriber.build(),
                      feCon,
                      xMsgConstants.REMOVE_SUBSCRIBER,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }


    @Test
    public void sendHostRemoval() throws Exception {
        driver.removeAllRegistrationFE("10.2.9.1", "10.2.9.1_node");

        assertRequest("10.2.9.1_node",
                      "10.2.9.1",
                      feCon,
                      xMsgConstants.REMOVE_ALL_REGISTRATION,
                      xMsgConstants.REMOVE_REQUEST_TIMEOUT);
    }



    @Test
    public void sendLocalPublisherFind() throws Exception {
        driver.findLocal("10.2.9.1_node", publisher.build(), true);

        assertRequest("10.2.9.1_node",
                      publisher.build(),
                      localCon,
                      xMsgConstants.FIND_PUBLISHER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void sendLocalSubscriberFind() throws Exception {
        driver.findLocal("10.2.9.1_node", subscriber.build(), false);

        assertRequest("10.2.9.1_node",
                      subscriber.build(),
                      localCon,
                      xMsgConstants.FIND_SUBSCRIBER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void sendFrontEndPublisherFind() throws Exception {
        driver.findGlobal("10.2.9.1_node", publisher.build(), true);

        assertRequest("10.2.9.1_node",
                      publisher.build(),
                      feCon,
                      xMsgConstants.FIND_PUBLISHER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void sendFrontEndSubscriberFind() throws Exception {
        driver.findGlobal("10.2.9.1_node", subscriber.build(), false);

        assertRequest("10.2.9.1_node",
                      subscriber.build(),
                      feCon,
                      xMsgConstants.FIND_SUBSCRIBER,
                      xMsgConstants.FIND_REQUEST_TIMEOUT);
    }


    @Test
    public void getRegistrationFromLocalFind() throws Exception {
        setResponse(new xMsgRegResponse("", "", registration));
        Set<xMsgRegistration> res = driver.findLocal("10.2.9.1_node", subscriber.build(), false);
        assertThat(res, is(registration));
    }


    @Test
    public void getRegistrationFromGlobalFind() throws Exception {
        setResponse(new xMsgRegResponse("", "", registration));
        Set<xMsgRegistration> res = driver.findGlobal("10.2.9.1_node", subscriber.build(), false);
        assertThat(res, is(registration));
    }


    private void assertRequest(String name, xMsgRegistration data, Socket socket,
                               xMsgConstants topic, xMsgConstants timeout)
            throws Exception {
        xMsgRegRequest request = new xMsgRegRequest(topic.getStringValue(), name, data);
        verify(driver).request(socket, request, timeout.getIntValue());
    }


    private void assertRequest(String name, String data, Socket socket,
                               xMsgConstants topic, xMsgConstants timeout)
            throws Exception {
        xMsgRegRequest request = new xMsgRegRequest(topic.getStringValue(), name, data);
        verify(driver).request(socket, request, timeout.getIntValue());
    }


    private void setResponse(xMsgRegResponse response) throws Exception {
        doReturn(response).when(driver).request(any(Socket.class),
                                                    any(xMsgRegRequest.class),
                                                    anyInt());
    }
}
