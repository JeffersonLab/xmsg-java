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

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jlab.coda.xmsg.data.xMsgD.xMsgData.Builder;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.testing.IntegrationTest;
import org.jlab.coda.xmsg.xsys.xMsgProxy;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.zeromq.ZContext;

import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@Category(IntegrationTest.class)
public class SubscriptionsTest {

    @Test
    public void unsuscribeStopsThread() throws Exception {

        xMsg actor = new xMsg("test", "localhost");
        xMsgConnection connection = actor.connect();

        xMsgSubscription subscription = actor.subscribe(connection, xMsgTopic.wrap("topic"), null);
        xMsgUtil.sleep(1000);
        actor.unsubscribe(subscription);

        assertFalse(subscription.isAlive());
    }


    @Test
    public void suscribeReceivesAllMessages() throws Exception {
        class Check {
            AtomicInteger counter = new AtomicInteger();
            AtomicLong sum = new AtomicLong();

            static final int N = 10000;
            static final long SUM_N = 49995000L;
        }

        final ZContext context = new ZContext();
        final Check check = new Check();

        Thread proxyThread = xMsgUtil.newThread("proxy-thread", new Runnable() {
            @Override
            public void run() {
                try {
                    xMsgProxy proxy = new xMsgProxy();
                    proxy.startProxy(context);
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        proxyThread.start();
        xMsgUtil.sleep(100);

        Thread subThread = xMsgUtil.newThread("sub-thread", new Runnable() {
            @Override
            public void run() {
                try {
                    xMsg actor = new xMsg("test_publisher", "localhost");
                    xMsgConnection connection = actor.connect();
                    xMsgUtil.sleep(100);
                    xMsgTopic topic = xMsgTopic.wrap("test_topic");
                    xMsgSubscription sub = actor.subscribe(connection, topic, new xMsgCallBack() {
                        @Override
                        public xMsgMessage callback(xMsgMessage msg) {
                            Builder data = (Builder) msg.getData();
                            int i = data.getFLSINT32();
                            check.counter.incrementAndGet();
                            check.sum.addAndGet(i);
                            return msg;
                        }
                    });
                    while (check.counter.get() < Check.N) {
                        xMsgUtil.sleep(100);
                    }
                    actor.unsubscribe(sub);
                } catch (SocketException | xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        subThread.start();
        xMsgUtil.sleep(100);

        Thread pubThread = xMsgUtil.newThread("pub-thread", new Runnable() {
            @Override
            public void run() {
                try {
                    xMsg actor = new xMsg("test_publisher", "localhost");
                    xMsgConnection connection = actor.connect();
                    xMsgUtil.sleep(100);
                    xMsgTopic topic = xMsgTopic.wrap("test_topic");
                    for (int i = 0; i < Check.N; i++) {
                        xMsgMessage msg = new xMsgMessage(topic, i);
                        actor.publish(connection, msg);
                    }
                } catch (IOException | xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        pubThread.start();

        subThread.join();
        pubThread.join();

        context.destroy();
        proxyThread.interrupt();
        proxyThread.join();

        assertThat(check.counter.get(), is(Check.N));
        assertThat(check.sum.get(), is(Check.SUM_N));
    }
}
