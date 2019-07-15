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

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.sys.pubsub.xMsgProxyDriver;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import java.util.function.Function;


public class ConnectionManagerTest {

    private xMsgConnectionFactory factory;
    private ConnectionManager manager;

    public ConnectionManagerTest() throws Exception {
        factory = mock(xMsgConnectionFactory.class);

        when(factory.createPublisherConnection(any(), any()))
                .thenAnswer(invocation -> {
                    xMsgProxyAddress a = (xMsgProxyAddress) invocation.getArguments()[0];
                    xMsgProxyDriver c = mock(xMsgProxyDriver.class);
                    when(c.getAddress()).thenReturn(a);
                    return c;
                });

        when(factory.createRegistrarConnection(any()))
                .thenAnswer(invocation -> {
                    xMsgRegAddress a = (xMsgRegAddress) invocation.getArguments()[0];
                    xMsgRegDriver d = mock(xMsgRegDriver.class);
                    when(d.getAddress()).thenReturn(a);
                    return d;
                });
    }

    @BeforeEach
    public void setup() {
        manager = new ConnectionManager(factory);
    }

    @Test
    public void createProxyConnections() throws Exception {
        createConnections(xMsgProxyAddress::new,
                          manager::getProxyConnection,
                          xMsgProxyDriver::getAddress);
    }

    @Test
    public void createRegistrarConnections() throws Exception {
        createConnections(xMsgRegAddress::new,
                          manager::getRegistrarConnection,
                          xMsgRegDriver::getAddress);
    }

    @Test
    public void reuseProxyConnections() throws Exception {

        reuseConnections(xMsgProxyAddress::new,
                         manager::getProxyConnection,
                         manager::releaseProxyConnection);
    }

    @Test
    public void reuseRegistrarConnections() throws Exception {

        reuseConnections(xMsgRegAddress::new,
                         manager::getRegistrarConnection,
                         manager::releaseRegistrarConnection);
    }

    private <A, C> void createConnections(Function<String, A> address,
                                          ConnectionBuilder<A, C> create,
                                          Function<C, A> inspect) throws Exception {
        A addr1 = address.apply("10.2.9.1");
        A addr2 = address.apply("10.2.9.2");

        C c1 = create.apply(addr1);
        C c2 = create.apply(addr2);
        C c3 = create.apply(addr2);

        assertThat(inspect.apply(c1), is(addr1));
        assertThat(inspect.apply(c2), is(addr2));
        assertThat(inspect.apply(c3), is(addr2));

        assertThat(c1, not(sameInstance(c2)));
        assertThat(c1, not(sameInstance(c3)));
        assertThat(c2, not(sameInstance(c3)));
    }

    private <A, C> void reuseConnections(Function<String, A> address,
                                         ConnectionBuilder<A, C> create,
                                         Consumer<C> release) throws Exception {
        A addr1 = address.apply("10.2.9.1");
        A addr2 = address.apply("10.2.9.2");

        C cc1 = create.apply(addr1);
        C cc2 = create.apply(addr2);
        C cc3 = create.apply(addr2);

        release.accept(cc1);
        release.accept(cc3);
        release.accept(cc2);

        C c1 = create.apply(addr1);
        C c2 = create.apply(addr2);
        C c3 = create.apply(addr2);

        assertThat(c1, is(sameInstance(cc1)));
        assertThat(c2, is(sameInstance(cc3)));
        assertThat(c3, is(sameInstance(cc2)));
    }

    @FunctionalInterface
    public interface ConnectionBuilder<A, C> {
        C apply(A a) throws xMsgException;
    }
}
