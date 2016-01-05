package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import java.util.function.Function;


public class ConnectionManagerTest {

    private xMsgConnectionFactory factory;
    private ConnectionManager manager;

    public ConnectionManagerTest() {
        factory = mock(xMsgConnectionFactory.class);

        when(factory.createProxyConnection(any(), any()))
                .thenAnswer(new Answer<xMsgConnection>() {
                    @Override
                    public xMsgConnection answer(InvocationOnMock invocation) throws Throwable {
                        xMsgConnection c = new xMsgConnection();
                        c.setAddress((xMsgAddress) invocation.getArguments()[0]);
                        return c;
                    }
                });
    }

    @Before
    public void setup() {
        manager = new ConnectionManager(factory);
    }

    @Test
    public void createProxyConnections() throws Exception {
        createConnections(xMsgAddress::new,
                          manager::getProxyConnection,
                          xMsgConnection::getAddress);
    }

    @Test
    public void reuseProxyConnections() throws Exception {

        reuseConnections(xMsgAddress::new,
                         manager::getProxyConnection,
                         manager::releaseProxyConnection);
    }


    private <A, C> void createConnections(Function<String, A> address,
                                          Function<A, C> create,
                                          Function<C, A> inspect) {
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
                                         Function<A, C> create,
                                         Consumer<C> release) {
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
}
