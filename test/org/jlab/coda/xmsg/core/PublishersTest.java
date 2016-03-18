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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.testing.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@Category(IntegrationTest.class)
public class PublishersTest {

    @Test
    public void suscribeReceivesAllMessages() throws Exception {
        class Check {
            AtomicInteger counter = new AtomicInteger();
            AtomicLong sum = new AtomicLong();

            static final int N = 100000;
            static final long SUM_N = 4999950000L;

        }

        final Check check = new Check();

        final String rawTopic = "test_topic";
        final CountDownLatch subReady = new CountDownLatch(1);

        ProxyThread proxyThread = new ProxyThread();
        proxyThread.start();


        Thread subThread = xMsgUtil.newThread("sub-thread", () -> {
            try {
                xMsg actor = new xMsg("test_subscriber");
                xMsgConnection connection = actor.createConnection();
                xMsgTopic topic = xMsgTopic.wrap(rawTopic);
                xMsgSubscription sub = actor.subscribe(connection, topic, msg -> {
                    int i = xMsgMessage.parseData(msg, Integer.class);
                    check.counter.incrementAndGet();
                    check.sum.addAndGet(i);
                });
                subReady.countDown();
                int counter = 0;
                while (check.counter.get() < Check.N) {
                    xMsgUtil.sleep(100);
                    if (++counter * 1000 > Check.N) {
                        break;
                    }
                }
                actor.unsubscribe(sub);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        });
        subThread.start();
        subReady.await();


        class Publisher implements Runnable {

            final int start;
            final int end;

            Publisher(int start, int n) {
                this.start = start;
                this.end = start + n;
            }

            @Override
            public void run() {
                try {
                    xMsg actor = new xMsg("test_publisher_" + start);
                    xMsgTopic topic = xMsgTopic.build(rawTopic, Integer.toString(start));
                    xMsgConnection connection = actor.createConnection();
                    for (int i = start; i < end; i++) {
                        xMsgMessage msg = xMsgMessage.createFrom(topic, i);
                        actor.publish(connection, msg);
                    }
                } catch (IOException | xMsgException e) {
                    e.printStackTrace();
                }
            }
        }

        final int numPublishers = 8;
        final int n = Check.N / numPublishers;
        List<Thread> publishers = new ArrayList<>();
        for (int i = 0; i < numPublishers; i++) {
            Thread pubThread = new Thread(new Publisher(i * n, n));
            publishers.add(pubThread);
            pubThread.start();
        }


        subThread.join();
        for (Thread pubThread : publishers) {
            pubThread.join();
        }

        proxyThread.stop();


        assertThat(check.counter.get(), is(Check.N));
        assertThat(check.sum.get(), is(Check.SUM_N));
    }


    @Test
    public void syncSuscribeReceivesAllMessages() throws Exception {
        class Check {
            AtomicInteger counter = new AtomicInteger();
            AtomicLong sum = new AtomicLong();

            static final int N = 1000;
            static final long SUM_N = 499500L;

        }

        final Check check = new Check();

        final String rawTopic = "test_topic";
        final CountDownLatch subReady = new CountDownLatch(1);

        ProxyThread proxyThread = new ProxyThread();
        proxyThread.start();


        Thread subThread = xMsgUtil.newThread("sub-thread", () -> {
            try {
                xMsg actor = new xMsg("test_reply_subscriber");
                xMsgConnection connection = actor.createConnection();
                xMsgTopic topic = xMsgTopic.wrap(rawTopic);
                xMsgSubscription sub = actor.subscribe(connection, topic, msg -> {
                    try {
                        xMsgConnection pubConnection = actor.getConnection();
                        try {
                            xMsgMessage res = xMsgMessage.createResponse(msg);
                            actor.publish(pubConnection, res);
                        } finally {
                            actor.releaseConnection(pubConnection);
                        }
                    } catch (xMsgException e) {
                        e.printStackTrace();
                    }
                });
                subReady.countDown();
                while (check.counter.get() < Check.N) {
                    xMsgUtil.sleep(100);
                }
                actor.unsubscribe(sub);
            } catch (xMsgException e) {
                e.printStackTrace();
            }
        });
        subThread.start();
        subReady.await();


        class Publisher implements Runnable {

            final int start;
            final int end;

            Publisher(int start, int n) {
                this.start = start;
                this.end = start + n;
            }

            @Override
            public void run() {
                try {
                    xMsg actor = new xMsg("test_sync_publisher_" + start);
                    xMsgTopic topic = xMsgTopic.build(rawTopic, Integer.toString(start));
                    for (int i = start; i < end; i++) {
                        xMsgConnection connection = actor.getConnection();
                        try {
                            xMsgMessage msg = xMsgMessage.createFrom(topic, i);
                            xMsgMessage res = actor.syncPublish(connection, msg, 1000);
                            Integer r = xMsgMessage.parseData(res, Integer.class);
                            check.counter.incrementAndGet();
                            check.sum.addAndGet(r);
                        } finally {
                            actor.releaseConnection(connection);
                        }
                    }
                } catch (IOException | xMsgException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        }

        final int numPublishers = 4;
        final int n = Check.N / numPublishers;
        List<Thread> publishers = new ArrayList<>();
        for (int i = 0; i < numPublishers; i++) {
            Thread pubThread = new Thread(new Publisher(i * n, n));
            publishers.add(pubThread);
            pubThread.start();
        }


        subThread.join();
        for (Thread pubThread : publishers) {
            pubThread.join();
        }

        proxyThread.stop();


        assertThat(check.counter.get(), is(Check.N));
        assertThat(check.sum.get(), is(Check.SUM_N));
    }


    private abstract static class TestRunner implements AutoCloseable {

        final ProxyThread proxyThread = new ProxyThread();
        final String rawTopic = "test_topic";
        final xMsg pubActor;

        TestRunner(boolean singlePubActor) {
            this.pubActor = singlePubActor ? new xMsg("test_publisher") : null;
            this.proxyThread.start();
        }

        abstract void receive(xMsg actor, xMsgMessage msg, Check check) throws Exception;

        abstract void publish(xMsg actor, xMsgMessage msg, Check check) throws Exception;

        void run(int totalMessages, int numPublishers) throws Exception {

            final Check check = new Check(totalMessages);
            final CountDownLatch subReady = new CountDownLatch(1);

            Thread subThread = xMsgUtil.newThread("sub-thread", () -> {
                try {
                    xMsg actor = new xMsg("test_subscriber");
                    xMsgConnection connection = actor.createConnection();
                    xMsgTopic topic = xMsgTopic.wrap(rawTopic);
                    xMsgSubscription sub = actor.subscribe(connection, topic, msg -> {
                        try {
                            receive(actor, msg, check);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    subReady.countDown();
                    wait(check);
                    actor.unsubscribe(sub);
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            });
            subThread.start();
            subReady.await();

            final int numMessages = check.n / numPublishers;
            List<Thread> publishers = new ArrayList<>(numPublishers);

            for (int i = 0; i < numPublishers; i++) {
                final int start = i * numMessages;
                final int end = start + numMessages;

                Thread pubThread = xMsgUtil.newThread("pub-" + start, () -> {
                    try {
                        xMsg actor = pubActor;
                        if (actor == null) {
                            actor = new xMsg("test_publisher_" + start);
                        }
                        xMsgTopic topic = xMsgTopic.build(rawTopic, Integer.toString(start));
                        for (int j = start; j < end; j++) {
                            xMsgMessage msg = xMsgMessage.createFrom(topic, j);
                            publish(actor, msg, check);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                publishers.add(pubThread);
                pubThread.start();
            }

            subThread.join();
            for (Thread pubThread : publishers) {
                pubThread.join();
            }

            assertThat(check.counter.get(), is(check.n));
            assertThat(check.sum.get(), is(check.total));
        }

        private void wait(Check check) {
            int counter = 0;
            while (check.counter.get() < check.n && counter < 10000) {
                xMsgUtil.sleep(100);
                counter += 100;
            }
        }

        @Override
        public void close() {
            try {
                proxyThread.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static class AsyncRunner extends TestRunner {

        AsyncRunner(boolean singlePubActor) {
            super(singlePubActor);
        }

        @Override
        void receive(xMsg actor, xMsgMessage msg, Check check) throws Exception {
            int i = xMsgMessage.parseData(msg, Integer.class);
            check.increment(i);
        }

        @Override
        void publish(xMsg actor, xMsgMessage msg, Check check) throws Exception {
            xMsgConnection connection = actor.getConnection();
            try {
                actor.publish(connection, msg);
            } finally {
                actor.releaseConnection(connection);
            }
        }
    }


    private static class SyncRunner extends TestRunner {

        SyncRunner(boolean singlePubActor) {
            super(singlePubActor);
        }

        @Override
        void receive(xMsg actor, xMsgMessage msg, Check check) throws Exception {
            xMsgConnection connection = actor.getConnection();
            try {
                xMsgMessage res = xMsgMessage.createResponse(msg);
                actor.publish(connection, res);
            } finally {
                actor.releaseConnection(connection);
            }
        }

        @Override
        void publish(xMsg actor, xMsgMessage msg, Check check) throws Exception {
            xMsgConnection connection = actor.getConnection();
            try {
                xMsgMessage res = actor.syncPublish(connection, msg, 1000);
                Integer r = xMsgMessage.parseData(res, Integer.class);
                check.increment(r);
            } finally {
                actor.releaseConnection(connection);
            }
        }
    }


    private static class Check {
        final int n;
        final long total;

        AtomicInteger counter = new AtomicInteger();
        AtomicLong sum = new AtomicLong();

        Check(int n) {
            long sum = 0;
            for (int i = 0; i < n; i++) {
                sum += i;
            }
            this.n = n;
            this.total = sum;
        }

        void increment(int i) {
            counter.incrementAndGet();
            sum.addAndGet(i);
        }
    }
}
