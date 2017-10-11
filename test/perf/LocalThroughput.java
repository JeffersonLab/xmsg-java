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

package perf;

import java.util.concurrent.CountDownLatch;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;

public final class LocalThroughput {

    private LocalThroughput() { }

    public static void main(String[] argv) {
        class Timer {
            int nr;
            long watch;
            long elapsed;
        }

        if (argv.length != 3) {
            printf("usage: local_thr <bind-to> <message-size> <message-count>\n");
            System.exit(1);
        }

        final String bindTo = argv[0];
        final int messageSize = Integer.parseInt(argv[1]);
        final long messageCount = Long.parseLong(argv[2]);

        final CountDownLatch finished = new CountDownLatch(1);
        final Timer timer = new Timer();

        try (xMsg subscriber = new xMsg("throughput_subscriber", 1)) {

            xMsgProxyAddress address = new xMsgProxyAddress(bindTo);
            xMsgTopic topic = xMsgTopic.wrap("thr_topic");

            xMsgSubscription sub = subscriber.subscribe(address, topic, msg -> {
                int size = msg.getDataSize();
                if (size != messageSize) {
                    printf("Message of incorrect size received " + size);
                    System.exit(1);
                }
                int nr = ++timer.nr;
                if (nr == 1) {
                    timer.watch = startClock();
                } else if (nr == messageCount) {
                    timer.elapsed = stopClock(timer.watch);
                    finished.countDown();
                }
            });

            System.out.println("Waiting for messages...");
            finished.await();

            if (timer.elapsed == 0) {
                timer.elapsed = 1;
            }

            long throughput = (long) (messageCount / (double) timer.elapsed * 1000000L);
            double megabits = (double) (throughput * messageSize * 8) / 1000000;
            double latency = (double) timer.elapsed / (messageCount);

            printf("Message elapsed: %.3f [s]%n", (double) timer.elapsed / 1000000L);
            printf("Message size: %d [B]%n", messageSize);
            printf("Message count: %d%n", (int) messageCount);
            printf("Mean transfer time: %.3f [us]%n", latency);
            printf("Mean transfer rate: %d [msg/s]%n", (int) throughput);
            printf("Mean throughput: %.3f [Mb/s]%n", megabits);

            subscriber.unsubscribe(sub);

        } catch (xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void printf(String string) {
        System.out.print(string);
    }


    private static void printf(String str, Object... args) {
        System.out.print(String.format(str, args));
    }

    public static long startClock() {
        return System.nanoTime();
    }

    public static long stopClock(long watch) {
        return (System.nanoTime() - watch) / 1000;
    }
}
