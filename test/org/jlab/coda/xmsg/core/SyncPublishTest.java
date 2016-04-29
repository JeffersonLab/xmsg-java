package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegInfo;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegQuery;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegRecord;

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

    /**
     * Replies back any received message.
     */
    private void listener(int poolSize) {
        String name = xMsgUtil.localhost();
        xMsgTopic topic = xMsgTopic.build(TOPIC, name);
        try (xMsg actor = new xMsg(name, regAddress, poolSize)) {
            xMsgConnection subCon = actor.getConnection();
            try {
                actor.register(xMsgRegInfo.subscriber(topic, "test subscriber"));
                System.out.printf("Registered with %s%n", regAddress);
                actor.subscribe(subCon, topic, (msg) -> {
                    try {
                        xMsgConnection repCon = actor.getConnection();
                        try {
                            xMsgMessage res = xMsgMessage.createResponse(msg);
                            actor.publish(repCon, res);
                        } finally {
                            actor.releaseConnection(repCon);
                        }
                    } catch (xMsgException e) {
                        e.printStackTrace();
                    }
                });
                System.out.printf("Using %d cores to reply requests...%n", poolSize);
                xMsgUtil.keepAlive();
            } catch (xMsgException e) {
                actor.destroyConnection(subCon);
                throw e;
            }
        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }

    private void publisher(int cores, int numMessages) {
        int chunkSize = numMessages / cores;
        int totalMessages = chunkSize * cores;
        long totalSum = getTotalSum(totalMessages);

        AtomicLong resSum = new AtomicLong();
        ThreadPoolExecutor pool = xMsgUtil.newFixedThreadPool(cores, "sync-pub-");

        int numListeners = 0;
        long startTime = 0;
        long endTime = 0;

        try (xMsg actor = new xMsg("sync_tester", regAddress)) {
            xMsgRegQuery query = xMsgRegQuery.subscribers().withDomain(TOPIC);
            Set<xMsgRegRecord> listeners = actor.discover(query);
            numListeners = listeners.size();
            if (numListeners == 0) {
                System.out.printf("No subscribers registered on %s%n", regAddress);
                System.out.println("Exiting...");
                return;
            }

            System.out.printf("Found %d subscribers registered on %s%n",
                              listeners.size(), regAddress);
            System.out.printf("Using %d cores to send %d messages to every subscriber...%n",
                               cores, totalMessages);

            startTime = System.currentTimeMillis();
            for (int i = 0; i < cores; i++) {
                final int start = i * chunkSize;
                final int end = start + chunkSize;
                pool.submit(() -> {
                    try {
                        for (int j = start; j < end; j++) {
                            for (xMsgRegRecord reg : listeners) {
                                xMsgMessage data = xMsgMessage.createFrom(reg.topic(), j);
                                xMsgConnection pubCon = actor.getConnection(reg.address());
                                try {
                                    xMsgMessage res = actor.syncPublish(pubCon, data, TIME_OUT);
                                    int value = xMsgMessage.parseData(res, Integer.class);
                                    resSum.addAndGet(value);
                                } catch (TimeoutException e) {
                                    e.printStackTrace();
                                } finally {
                                    actor.releaseConnection(pubCon);
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
                System.err.println("execution pool did not terminate");
            } else {
                endTime = System.currentTimeMillis();
            }
        } catch (xMsgException | InterruptedException e) {
            e.printStackTrace();
        }

        totalSum *= numListeners;

        if (resSum.get() == totalSum) {
            System.out.println("OK: all messages received.");
            System.out.println("Total messages: " + totalMessages * numListeners);

            double duration = (endTime - startTime) / 1000.0;
            double average = 1.0 * (endTime - startTime) / (totalMessages * numListeners);
            System.out.printf("Total time: %.2f [s]%n", duration);
            System.out.printf("Average time: %.2f [ms]%n", average);
        } else {
            System.out.printf("ERROR: expected = %d  received = %d%n", totalSum, resSum.get());
            System.exit(1);
        }
    }

    private long getTotalSum(int numMessages) {
        long sum = 0;
        for (int i = 0; i < numMessages; i++) {
            sum += i;
        }
        return sum;
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

        SyncPublishTest test = new SyncPublishTest(frontEnd);
        if (command.equals("listener")) {
            int poolSize = Integer.parseInt(cores);
            test.listener(poolSize);
        } else {
            int pubThreads = Integer.parseInt(cores);
            int totalMessages = Integer.parseInt(command);
            test.publisher(pubThreads, totalMessages);
        }
    }
}
