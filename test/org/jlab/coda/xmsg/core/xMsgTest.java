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

import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegInfo;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegQuery;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void setup() throws Exception {
        xMsgConnectionFactory factory = mock(xMsgConnectionFactory.class);

        driver = mock(xMsgRegDriver.class);
        core = new xMsg(name, new xMsgProxyAddress(), regAddr, factory, 1);

        doReturn(new xMsgRegAddress()).when(driver).getAddress();
        doReturn(driver).when(factory).createRegistrarConnection(any(xMsgRegAddress.class));
    }


    @Test
    public void registerPublisher() throws Exception {
        core.register(xMsgRegInfo.publisher(topic, "test pub"), regAddr, 1000);

        xMsgRegistration.Builder expected = createRegistration(topic, true);
        expected.setDescription("test pub");

        verify(driver).addRegistration(eq(name), eq(expected.build()), eq(1000));
    }


    @Test
    public void registerSubscriber() throws Exception {
        core.register(xMsgRegInfo.subscriber(topic, "test sub"), regAddr, 1000);

        xMsgRegistration.Builder expected = createRegistration(topic, false);
        expected.setDescription("test sub");

        verify(driver).addRegistration(eq(name), eq(expected.build()), eq(1000));
    }


    @Test
    public void removePublisher() throws Exception {
        core.deregister(xMsgRegInfo.publisher(topic), regAddr, 1500);

        xMsgRegistration.Builder expected = createRegistration(topic, true);

        verify(driver).removeRegistration(eq(name), eq(expected.build()), eq(1500));
    }


    @Test
    public void removeSubscriber() throws Exception {
        core.deregister(xMsgRegInfo.subscriber(topic), regAddr, 1500);

        xMsgRegistration.Builder expected = createRegistration(topic, false);

        verify(driver).removeRegistration(eq(name), eq(expected.build()), eq(1500));
    }


    @Test
    public void findPublishers() throws Exception {
        core.discover(xMsgRegQuery.publishers(topic), regAddr, 2000);

        xMsgRegistration.Builder expected = createQuery(topic, true);

        verify(driver).findRegistration(eq(name), eq(expected.build()), eq(2000));
    }


    @Test
    public void findSubscribers() throws Exception {
        core.discover(xMsgRegQuery.subscribers(topic), regAddr, 2000);

        xMsgRegistration.Builder expected = createQuery(topic, false);

        verify(driver).findRegistration(eq(name), eq(expected.build()), eq(2000));
    }


    private xMsgRegistration.Builder createRegistration(xMsgTopic topic, boolean isPublisher) {
        return RegistrationDataFactory.newRegistration(name, topic.toString(), isPublisher);
    }


    private xMsgRegistration.Builder createQuery(xMsgTopic topic, boolean isPublisher) {
        String udf = xMsgConstants.UNDEFINED;
        return RegistrationDataFactory.newRegistration(udf, udf, topic.toString(), isPublisher);
    }
}
