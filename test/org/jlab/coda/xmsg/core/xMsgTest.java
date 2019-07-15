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

package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.data.xMsgRegInfo;
import org.jlab.coda.xmsg.data.xMsgRegQuery;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;
import org.jlab.coda.xmsg.sys.regdis.RegistrationDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType.PUBLISHER;
import static org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType.SUBSCRIBER;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class xMsgTest {

    private xMsgRegDriver driver;
    private xMsg core;

    private final String name = "asimov";
    private final xMsgTopic topic = xMsgTopic.wrap("writer:scifi:book");
    private final xMsgRegAddress regAddr = new xMsgRegAddress();

    @BeforeEach
    public void setup() throws Exception {
        xMsgConnectionFactory factory = mock(xMsgConnectionFactory.class);
        xMsgSetup setup = xMsgSetup.newBuilder()
                                   .withRegistrar(regAddr)
                                   .withPoolSize(1)
                                   .build();

        driver = mock(xMsgRegDriver.class);
        core = new xMsg(name, setup, factory);

        doReturn(new xMsgRegAddress()).when(driver).getAddress();
        doReturn(driver).when(factory).createRegistrarConnection(any(xMsgRegAddress.class));
    }


    @Test
    public void registerPublisher() throws Exception {
        core.register(xMsgRegInfo.publisher(topic, "test pub"), regAddr, 1000);

        xMsgRegistration.Builder expected = createRegistration(PUBLISHER, topic);
        expected.setDescription("test pub");

        verify(driver).addRegistration(eq(name), eq(expected.build()), eq(1000L));
    }


    @Test
    public void registerSubscriber() throws Exception {
        core.register(xMsgRegInfo.subscriber(topic, "test sub"), regAddr, 1000);

        xMsgRegistration.Builder expected = createRegistration(SUBSCRIBER, topic);
        expected.setDescription("test sub");

        verify(driver).addRegistration(eq(name), eq(expected.build()), eq(1000L));
    }


    @Test
    public void removePublisher() throws Exception {
        core.deregister(xMsgRegInfo.publisher(topic), regAddr, 1500);

        xMsgRegistration.Builder expected = createRegistration(PUBLISHER, topic);

        verify(driver).removeRegistration(eq(name), eq(expected.build()), eq(1500L));
    }


    @Test
    public void removeSubscriber() throws Exception {
        core.deregister(xMsgRegInfo.subscriber(topic), regAddr, 1500);

        xMsgRegistration.Builder expected = createRegistration(SUBSCRIBER, topic);

        verify(driver).removeRegistration(eq(name), eq(expected.build()), eq(1500L));
    }


    @Test
    public void findPublishers() throws Exception {
        xMsgRegQuery query = xMsgRegQuery.publishers(topic);

        core.discover(query, regAddr, 2000);

        verify(driver).findRegistration(eq(name), eq(query.data().build()), eq(2000L));
    }


    @Test
    public void findSubscribers() throws Exception {
        xMsgRegQuery query = xMsgRegQuery.subscribers(topic);

        core.discover(query, regAddr, 2000);

        verify(driver).findRegistration(eq(name), eq(query.data().build()), eq(2000L));
    }


    @Test
    public void filterPublishers() throws Exception {
        xMsgRegQuery query = xMsgRegQuery.publishers().withDomain("domain");

        core.discover(query, regAddr, 2000);

        verify(driver).filterRegistration(eq(name), eq(query.data().build()), eq(2000L));
    }


    @Test
    public void filterSubscribers() throws Exception {
        xMsgRegQuery query = xMsgRegQuery.subscribers().withSubject("subject");

        core.discover(query, regAddr, 2000);

        verify(driver).filterRegistration(eq(name), eq(query.data().build()), eq(2000L));
    }


    @Test
    public void allPublishers() throws Exception {
        xMsgRegQuery query = xMsgRegQuery.publishers().all();

        core.discover(query, regAddr, 2000);

        verify(driver).allRegistration(eq(name), eq(query.data().build()), eq(2000L));
    }


    @Test
    public void allSubscribers() throws Exception {
        xMsgRegQuery query = xMsgRegQuery.subscribers().all();

        core.discover(query, regAddr, 2000);

        verify(driver).allRegistration(eq(name), eq(query.data().build()), eq(2000L));
    }


    private xMsgRegistration.Builder createRegistration(xMsgRegistration.OwnerType regType,
                                                        xMsgTopic topic) {
        return RegistrationDataFactory.newRegistration(name, regType, topic.toString());
    }
}
