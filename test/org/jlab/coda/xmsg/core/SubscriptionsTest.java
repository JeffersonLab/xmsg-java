/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.testing.IntegrationTest;
import org.jlab.coda.xmsg.xsys.xMsgProxy;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.zeromq.ZContext;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@Category(IntegrationTest.class)
public class SubscriptionsTest {

    @Test
    public void unsuscribeStopsThread() throws Exception {

        xMsg actor = new xMsg("test");
        xMsgConnection con = actor.connect();

        xMsgSubscription subscription = actor.subscribe(con, xMsgTopic.wrap("topic"), null);
        xMsgUtil.sleep(1000);
        actor.unsubscribe(subscription);

        assertFalse(subscription.isAlive());
    }


    @Test
    public void suscribeReceivesAllMessages() throws Exception {
        class Check {
            static final int N = 10000;
            static final long SUM_N = 49995000L;
            AtomicInteger counter = new AtomicInteger();
            AtomicLong sum = new AtomicLong();
        }

        final Check check = new Check();

        ProxyThread proxyThread = new ProxyThread();
        proxyThread.start();

        Thread subThread = xMsgUtil.newThread("sub-thread", new Runnable() {
            @Override
            public void run() {
                try {
                    xMsg actor = new xMsg("test_publisher");
                    xMsgConnection con = actor.connect();
                    xMsgUtil.sleep(100);
                    xMsgTopic topic = xMsgTopic.wrap("test_topic");
                    xMsgSubscription sub = actor.subscribe(con, topic, new xMsgCallBack() {
                        @Override
                        public xMsgMessage callback(xMsgMessage msg) {
                            int i = parseData(msg);
                            check.counter.incrementAndGet();
                            check.sum.addAndGet(i);
                            return msg;
                        }
                    });
                    while (check.counter.get() < Check.N) {
                        xMsgUtil.sleep(100);
                    }
                    actor.unsubscribe(sub);
                } catch (xMsgException e) {
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
                    xMsg actor = new xMsg("test_publisher");
                    xMsgConnection con = actor.connect();

                    xMsgUtil.sleep(100);
                    xMsgTopic topic = xMsgTopic.wrap("test_topic");
                    for (int i = 0; i < Check.N; i++) {
                        xMsgMessage msg = xMsgMessage.createFrom(topic, i);
                        actor.publish(con, msg);
                    }
                } catch (IOException | xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        pubThread.start();

        subThread.join();
        pubThread.join();

        proxyThread.stop();

        assertThat(check.counter.get(), is(Check.N));
        assertThat(check.sum.get(), is(Check.SUM_N));
    }


    @Test
    public void syncPublicationReceivesAllResponses() throws Exception {
        class Check {
            static final int N = 100;
            static final long SUM_N = 4950L;
            int counter = 0;
            long sum = 0;
        }

        final Check check = new Check();

        ProxyThread proxyThread = new ProxyThread();
        proxyThread.start();

        Thread pubThread = xMsgUtil.newThread("syncpub-thread", new Runnable() {
            @Override
            public void run() {
                try {
                    final xMsg subActor = new xMsg("test_publisher");
                    final xMsgConnection scon = subActor.connect();

                    xMsgUtil.sleep(100);
                    xMsgTopic subTopic = xMsgTopic.wrap("test_topic");
                    xMsgSubscription sub = subActor.subscribe(scon, subTopic, new xMsgCallBack() {
                        @Override
                        public xMsgMessage callback(xMsgMessage msg) {
                            try {
                                xMsgMessage response = xMsgMessage.createResponse(msg);
                                subActor.publish(scon, response);
                            } catch (xMsgException e) {
                                e.printStackTrace();
                            }
                            return msg;
                        }
                    });
                    xMsgUtil.sleep(100);
                    xMsg pubActor = new xMsg("test_publisher");
                    xMsgConnection pcon = subActor.connect();
                    xMsgUtil.sleep(100);
                    xMsgTopic pubTopic = xMsgTopic.wrap("test_topic");
                    for (int i = 0; i < Check.N; i++) {
                        xMsgMessage msg = xMsgMessage.createFrom(pubTopic, i);
                        xMsgMessage resMsg = pubActor.syncPublish(pcon, msg, 1000);
                        int data = parseData(resMsg);
                        check.sum += data;
                        check.counter++;
                    }
                    subActor.unsubscribe(sub);
                } catch (IOException | xMsgException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
        pubThread.start();

        pubThread.join();
        proxyThread.stop();

        assertThat(check.counter, is(Check.N));
        assertThat(check.sum, is(Check.SUM_N));
    }


    @Test
    public void syncPublicationThrowsOnTimeout() throws Exception {
        class Check {
            boolean received = false;
            boolean timeout = false;
        }

        final Check check = new Check();

        ProxyThread proxyThread = new ProxyThread();
        proxyThread.start();

        Thread pubThread = xMsgUtil.newThread("syncpub-thread", new Runnable() {
            @Override
            public void run() {
                try {
                    xMsg subActor = new xMsg("test_publisher");
                    xMsgConnection scon = subActor.connect();

                    xMsgUtil.sleep(100);
                    xMsgTopic subTopic = xMsgTopic.wrap("test_topic");
                    xMsgSubscription sub = subActor.subscribe(scon, subTopic, new xMsgCallBack() {
                        @Override
                        public xMsgMessage callback(xMsgMessage msg) {
                            check.received = true;
                            xMsgUtil.sleep(1500);
                            return msg;
                        }
                    });
                    xMsgUtil.sleep(100);
                    xMsg pubActor = new xMsg("test_publisher");
                    xMsgConnection pcon = pubActor.connect();
                    xMsgUtil.sleep(100);
                    xMsgTopic pubTopic = xMsgTopic.wrap("test_topic");
                    xMsgMessage msg = xMsgMessage.createFrom(pubTopic, 1);
                    try {
                        pubActor.syncPublish(pcon, msg, 1000);
                    } catch (TimeoutException e) {
                        check.timeout = true;
                    }
                    subActor.unsubscribe(sub);
                } catch (IOException | xMsgException e) {
                    e.printStackTrace();
                }
            }
        });
        pubThread.start();
        pubThread.join();

        proxyThread.stop();

        assertTrue("not received", check.received);
        assertTrue("no timeout", check.timeout);
    }


    private class ProxyThread {

        private final ZContext context = new ZContext();
        private final Thread proxyThread;

        public ProxyThread() {
            proxyThread = xMsgUtil.newThread("proxy-thread", new Runnable() {
                    @Override
                    public void run() {
                        try {
                            xMsgProxy proxy = new xMsgProxy(context);
                            proxy.start();
                        } catch (xMsgException e) {
                            e.printStackTrace();
                        }
                    }
            });
        }

        public void start() {
            proxyThread.start();
            xMsgUtil.sleep(100);
        }

        public void stop() throws InterruptedException {
            context.destroy();
            proxyThread.interrupt();
            proxyThread.join();
        }
    }


    private int parseData(xMsgMessage msg) {
        try {
            xMsgData data = xMsgData.parseFrom(msg.getData());
            return data.getFLSINT32();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
