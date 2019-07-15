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
import org.jlab.coda.xmsg.data.xMsgRegRecord;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.sys.ProxyWrapper;
import org.jlab.coda.xmsg.sys.RegistrarWrapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Tests multithread sync-publication of messages to multiple registered
 * subscribers, distributed on multiple nodes (when running from command-line).
 * Uses N cores to concurrently send M messages to each subscriber.
 * <p>
 * The number of subscribers will be obtained from the registration database.
 * The publisher will use N threads in parallel, and each thread will be sending
 * a subset of M/N messages to each subscriber. A subscriber must received and
 * respond all M messages.
 * <p>
 * The messages are a unique sequence of integers from 0 to M-1.
 * Their sum is used to check that all messages were delivered.
 *
 * @see SubscriptionsTest
 */
@Tag("integration")
public final class SyncPublishTest {

    private static final String TOPIC = "sync_pub_test";
    private static final int TIME_OUT = 1000;

    private static xMsg listener(int poolSize, String name, xMsgRegAddress regAddress)
            throws xMsgException {
        xMsgTopic topic = xMsgTopic.build(TOPIC, name);
        xMsg actor = new xMsg(name, regAddress, poolSize);
        try {
            actor.register(xMsgRegInfo.subscriber(topic, "test subscriber"));
            System.out.printf("Registered %s with %s%n", topic, regAddress);
            actor.subscribe(topic, msg -> {
                try {
                    actor.publish(xMsgMessage.createResponse(msg));
                } catch (xMsgException e) {
                    e.printStackTrace();
                }
            });
            System.out.printf("Using %d cores to reply requests...%n", poolSize);
            return actor;
        } catch (xMsgException e) {
            actor.close();
            throw e;
        }
    }

    private static Result publisher(int cores, int numMessages, xMsgRegAddress regAddress)
            throws Exception {
        ThreadPoolExecutor pool = xMsgUtil.newThreadPool(cores, "sync-pub-");

        try (xMsg actor = new xMsg("sync_tester", regAddress)) {
            xMsgRegQuery query = xMsgRegQuery.subscribers().withDomain(TOPIC);
            Set<xMsgRegRecord> listeners = actor.discover(query);
            int numListeners = listeners.size();
            if (numListeners == 0) {
                throw new RuntimeException("no subscribers registered on" + regAddress);
            }

            Result results = new Result(cores, numListeners, numMessages);

            System.out.printf("Found %d subscribers registered on %s%n",
                              numListeners, regAddress);
            System.out.printf("Using %d cores to send %d messages to every subscriber...%n",
                              cores, results.totalMessages);

            results.startClock();
            for (int i = 0; i < cores; i++) {
                final int start = i * results.chunkSize;
                final int end = start + results.chunkSize;
                pool.submit(() -> {
                    try {
                        for (int j = start; j < end; j++) {
                            for (xMsgRegRecord reg : listeners) {
                                try (xMsgConnection pubCon = actor.getConnection(reg.address())) {
                                    xMsgMessage data = xMsgMessage.createFrom(reg.topic(), j);
                                    xMsgMessage res = actor.syncPublish(pubCon, data, TIME_OUT);
                                    int value = xMsgMessage.parseData(res, Integer.class);
                                    results.add(value);
                                } catch (TimeoutException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    } catch (xMsgException e) {
                        e.printStackTrace();
                    }
                });
            }

            pool.shutdown();
            if (!pool.awaitTermination(5, TimeUnit.MINUTES)) {
                throw new RuntimeException("execution pool did not terminate");
            } else {
                results.stopClock();
            }

            return results;
        } catch (xMsgException | InterruptedException e) {
            throw e;
        }
    }

    private static final class Result {

        final int numListeners;
        final int chunkSize;
        final int totalMessages;
        final long totalSum;

        long startTime;
        long endTime;

        final AtomicLong sum = new AtomicLong();

        private Result(int cores, int numListeners, int numMessages) {
            this.numListeners = numListeners;
            this.chunkSize = numMessages / cores;
            this.totalMessages = chunkSize * cores;
            this.totalSum = getTotalSum(totalMessages) * numListeners;
        }

        private long getTotalSum(int numMessages) {
            long sum = 0;
            for (int i = 0; i < numMessages; i++) {
                sum += i;
            }
            return sum;
        }

        public void startClock() {
            startTime = System.currentTimeMillis();
        }

        public void stopClock() {
            endTime = System.currentTimeMillis();
        }

        public void add(int value) {
            sum.addAndGet(value);
        }

        public boolean check() {
            if (sum.get() == totalSum) {
                System.out.println("OK: all messages received.");
                System.out.println("Total messages: " + totalMessages * numListeners);

                double duration = (endTime - startTime) / 1000.0;
                double average = 1.0 * (endTime - startTime) / (totalMessages * numListeners);
                System.out.printf("Total time: %.2f [s]%n", duration);
                System.out.printf("Average time: %.2f [ms]%n", average);
                return true;
            } else {
                System.out.printf("ERROR: expected = %d  received = %d%n", totalSum, sum.get());
                return false;
            }
        }
    }

    @Test
    public void run() throws Exception {
        xMsgRegAddress address = new xMsgRegAddress();
        try (RegistrarWrapper registrar = new RegistrarWrapper();
             ProxyWrapper proxy = new ProxyWrapper()) {
            try (xMsg list1 = listener(1, "foo", address);
                 xMsg list2 = listener(1, "bar", address)) {
                xMsgUtil.sleep(100);
                Result results = publisher(1, 1000, address);
                assertThat(results.sum.get(), is(results.totalSum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("usage:");
            System.out.println(" sync_pub <fe_host> <pool_size> listener");
            System.out.println(" sync_pub <fe_host> <cores> <num_msg>");
            System.exit(1);
        }

        String frontEnd = args[0];
        String cores = args[1];
        String command = args[2];

        try {
            xMsgRegAddress address = new xMsgRegAddress(frontEnd);
            if (command.equals("listener")) {
                int poolSize = Integer.parseInt(cores);
                try (xMsg sub = SyncPublishTest.listener(poolSize, "local", address)) {
                    xMsgUtil.keepAlive();
                }
            } else {
                int pubThreads = Integer.parseInt(cores);
                int totalMessages = Integer.parseInt(command);
                boolean stat = SyncPublishTest.publisher(pubThreads, totalMessages, address)
                                              .check();
                if (!stat) {
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
