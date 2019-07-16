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
import org.jlab.coda.xmsg.sys.ProxyWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Simple pub-sub integration tests. Ensure that the subscription is running,
 * that a publisher can communicate with a subscriber through the same proxy,
 * and that all messages are published and received.
 * <p>
 * When counting published messages, each one contains an integer from a unique
 * sequence of integers, and their sum is used to check that all messages were
 * delivered.
 * <p>
 * For more complex cases see {@link PublishersTest} and {@link SyncPublishTest}.
 */
@Tag("integration")
public class SubscriptionsTest {

    private ProxyWrapper proxyThread;

    @BeforeEach
    public void setup() {
        proxyThread = new ProxyWrapper();
    }


    @AfterEach
    public void teardown() {
        proxyThread.close();
    }


    @Test
    public void unsubscribeStopsThread() throws Exception {
        try (xMsg actor = new xMsg("test")) {
            xMsgSubscription subscription = actor.subscribe(xMsgTopic.wrap("topic"), null);
            xMsgUtil.sleep(1000);
            actor.unsubscribe(subscription);

            assertFalse(subscription.isAlive());
        }
    }


    @Test
    public void subscribeReceivesAllMessages() throws Exception {
        class Check {
            static final int N = 10000;
            static final long SUM_N = 49995000L;
            AtomicInteger counter = new AtomicInteger();
            AtomicLong sum = new AtomicLong();
        }

        final Check check = new Check();

        Thread subThread = xMsgUtil.newThread("sub-thread", () -> {
            try (xMsg actor = new xMsg("test_subscriber")) {
                xMsgTopic topic = xMsgTopic.wrap("test_topic");
                xMsgSubscription sub = actor.subscribe(topic, msg -> {
                    int i = xMsgMessage.parseData(msg, Integer.class);
                    check.counter.incrementAndGet();
                    check.sum.addAndGet(i);
                });
                int shutdownCounter = 0;
                while (check.counter.get() < Check.N && shutdownCounter < 100) {
                    shutdownCounter++;
                    xMsgUtil.sleep(100);
                }
                actor.unsubscribe(sub);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        });
        subThread.start();
        xMsgUtil.sleep(100);

        Thread pubThread = xMsgUtil.newThread("pub-thread", () -> {
            try (xMsg actor = new xMsg("test_publisher");
                 xMsgConnection con = actor.getConnection()) {
                xMsgTopic topic = xMsgTopic.wrap("test_topic");
                for (int i = 0; i < Check.N; i++) {
                    xMsgMessage msg = xMsgMessage.createFrom(topic, i);
                    actor.publish(con, msg);
                }
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        });
        pubThread.start();

        subThread.join();
        pubThread.join();

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

        Thread pubThread = xMsgUtil.newThread("syncpub-thread", () -> {
            try (xMsg subActor = new xMsg("test_subscriber");
                 xMsg pubActor = new xMsg("test_publisher")) {
                xMsgTopic subTopic = xMsgTopic.wrap("test_topic");
                subActor.subscribe(subTopic, msg -> {
                    try {
                        subActor.publish(xMsgMessage.createResponse(msg));
                    } catch (xMsgException e) {
                        e.printStackTrace();
                    }
                });
                xMsgUtil.sleep(100);
                try (xMsgConnection pubCon = subActor.getConnection()) {
                    xMsgTopic pubTopic = xMsgTopic.wrap("test_topic");
                    for (int i = 0; i < Check.N; i++) {
                        xMsgMessage msg = xMsgMessage.createFrom(pubTopic, i);
                        xMsgMessage resMsg = pubActor.syncPublish(pubCon, msg, 1000);
                        int data = xMsgMessage.parseData(resMsg, Integer.class);
                        check.sum += data;
                        check.counter++;
                    }
                }
            } catch (xMsgException | TimeoutException e) {
                e.printStackTrace();
            }
        });
        pubThread.start();
        pubThread.join();

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

        Thread pubThread = xMsgUtil.newThread("syncpub-thread", () -> {
            try (xMsg subActor = new xMsg("test_subscriber");
                 xMsg pubActor = new xMsg("test_publisher")) {
                xMsgTopic subTopic = xMsgTopic.wrap("test_topic");
                xMsgSubscription sub = subActor.subscribe(subTopic, msg -> {
                    try {
                        check.received = true;
                        xMsgUtil.sleep(1500);
                        subActor.publish(xMsgMessage.createResponse(msg));
                    } catch (xMsgException e) {
                        e.printStackTrace();
                    }
                });
                xMsgUtil.sleep(100);
                try {
                    xMsgTopic pubTopic = xMsgTopic.wrap("test_topic");
                    xMsgMessage msg = xMsgMessage.createFrom(pubTopic, 1);
                    pubActor.syncPublish(msg, 1000);
                } catch (TimeoutException e) {
                    check.timeout = true;
                }
                subActor.unsubscribe(sub);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        });
        pubThread.start();
        pubThread.join();

        assertTrue(check.received, "Received message");
        assertTrue(check.timeout, "Timeout reached");
    }

    @Test
    public void multiTopicSubscriptionReceivesAllMessages() throws Exception {
        class Check {
            static final int N = 10000;
            static final long SUM_N = 49995000L;
            AtomicInteger counter = new AtomicInteger();
            AtomicLong sum = new AtomicLong();
        }

        final Check check = new Check();

        Thread subThread = xMsgUtil.newThread("sub-thread", () -> {
            try (xMsg actor = new xMsg("test_subscriber")) {
                Set<xMsgTopic> topics = new HashSet<>();
                topics.add(xMsgTopic.wrap("1_test_topic"));
                topics.add(xMsgTopic.wrap("2_test_topic"));
                xMsgSubscription sub = actor.subscribe(topics, msg -> {
                    int i = xMsgMessage.parseData(msg, Integer.class);
                    check.counter.incrementAndGet();
                    check.sum.addAndGet(i);
                });
                int shutdownCounter = 0;
                while (check.counter.get() < Check.N && shutdownCounter < 100) {
                    shutdownCounter++;
                    xMsgUtil.sleep(100);
                }
                actor.unsubscribe(sub);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        });
        subThread.start();
        xMsgUtil.sleep(100);

        Thread pubThread = xMsgUtil.newThread("pub-thread", () -> {
            try (xMsg actor = new xMsg("test_publisher");
                 xMsgConnection con = actor.getConnection()) {
                xMsgTopic[] topics = new xMsgTopic[] {
                        xMsgTopic.wrap("1_test_topic"), xMsgTopic.wrap("2_test_topic")
                };
                for (int i = 0; i < Check.N; i++) {
                    xMsgMessage msg = xMsgMessage.createFrom(topics[i % 2], i);
                    actor.publish(con, msg);
                }
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        });
        pubThread.start();

        subThread.join();
        pubThread.join();

        assertThat(check.counter.get(), is(Check.N));
        assertThat(check.sum.get(), is(Check.SUM_N));
    }
}
