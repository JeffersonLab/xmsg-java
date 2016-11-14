package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.data.xMsgRegInfo;
import org.jlab.coda.xmsg.data.xMsgRegQuery;
import org.jlab.coda.xmsg.data.xMsgRegRecord;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgRegAddress;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public final class SyncPublishTest {

    private static final String TOPIC = "sync_pub_test";
    private static final int TIME_OUT = 1000;

    private final xMsgRegAddress regAddress;

    private SyncPublishTest(String feHost) {
        regAddress = new xMsgRegAddress(feHost);
    }

    private xMsg listener(int poolSize) throws xMsgException {
        String name = xMsgUtil.localhost();
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

    private Result publisher(int cores, int numMessages) throws Exception {
        ThreadPoolExecutor pool = xMsgUtil.newFixedThreadPool(cores, "sync-pub-");

        try (xMsg actor = new xMsg("sync_tester", regAddress)) {
            xMsgRegQuery query = xMsgRegQuery.subscribers().withDomain(TOPIC);
            Set<xMsgRegRecord> listeners = actor.discover(query);
            int numListeners = listeners.size();
            if (numListeners == 0) {
                throw new RuntimeException("No subscribers registered on" + regAddress);
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

        public void check() {
            if (sum.get() == totalSum) {
                System.out.println("OK: all messages received.");
                System.out.println("Total messages: " + totalMessages * numListeners);

                double duration = (endTime - startTime) / 1000.0;
                double average = 1.0 * (endTime - startTime) / (totalMessages * numListeners);
                System.out.printf("Total time: %.2f [s]%n", duration);
                System.out.printf("Average time: %.2f [ms]%n", average);
            } else {
                System.out.printf("ERROR: expected = %d  received = %d%n", totalSum, sum.get());
                System.exit(1);
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
            SyncPublishTest test = new SyncPublishTest(frontEnd);
            if (command.equals("listener")) {
                int poolSize = Integer.parseInt(cores);
                try (xMsg sub = test.listener(poolSize)) {
                    xMsgUtil.keepAlive();
                }
            } else {
                int pubThreads = Integer.parseInt(cores);
                int totalMessages = Integer.parseInt(command);
                test.publisher(pubThreads, totalMessages).check();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
