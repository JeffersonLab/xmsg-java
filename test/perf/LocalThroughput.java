package perf;

import org.jlab.coda.xmsg.core.*;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import java.io.IOException;

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
        final long messageCount = Long.valueOf(argv[2]);

        final Object lock = new Object();
        final Timer timer = new Timer();

        try {
            final xMsg subscriber = new xMsg("throughput_subscriber", 1);
            xMsgConnection con = subscriber.connect(bindTo);
            final xMsgTopic topic = xMsgTopic.wrap("thr_topic");

            xMsgUtil.sleep(100);

            xMsgSubscription sub = subscriber.subscribe(con,
                    topic, new xMsgCallBack() {

                @Override
                public xMsgMessage callback(xMsgMessage msg) {
                    int size = msg.getDataSize();
                    if (size != messageSize) {
                        printf("message of incorrect size received " + size);
                        System.exit(1);
                    }
                    int nr = ++timer.nr;
                    if (nr == 1) {
                        timer.watch = startClock();
                    } else if (nr == messageCount) {
                        timer.elapsed = stopClock(timer.watch);
                        synchronized (lock) {
                            lock.notify();
                        }
                    }
                    return msg;
                }
            });

            synchronized (lock) {
                lock.wait();
            }
            if (timer.elapsed == 0) {
                timer.elapsed = 1;
            }

            long throughput = (long) (messageCount / (double) timer.elapsed * 1000000L);
            double megabits = (double) (throughput * messageSize * 8) / 1000000;
            double latency = (double) timer.elapsed / (messageCount);

            printf("message elapsed: %.3f [s]%n", (double) timer.elapsed / 1000000L);
            printf("message size: %d [B]%n", messageSize);
            printf("message count: %d%n", (int) messageCount);
            printf("mean transfer time: %.3f [us]%n", latency);
            printf("mean transfer rate: %d [msg/s]%n", (int) throughput);
            printf("mean throughput: %.3f [Mb/s]%n", megabits);

            subscriber.unsubscribe(sub);
            subscriber.destroy();

        } catch (IOException | xMsgException e) {
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


    private static void printf(String str, Object ... args) {
        System.out.print(String.format(str, args));
    }

    public static long startClock() {
        return System.nanoTime();
    }

    public static long stopClock(long watch) {
        return (System.nanoTime() - watch) / 1000;
    }
}
