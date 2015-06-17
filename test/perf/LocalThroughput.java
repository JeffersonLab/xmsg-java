package perf;

import java.net.SocketException;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgCallBack;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgAddress;
import org.jlab.coda.xmsg.net.xMsgConnection;

import zmq.ZMQ;

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
            final xMsg subscriber = new xMsg("throughput_subscriber", "localhost", 1);
            final xMsgAddress pubNode = new xMsgAddress(bindTo);
            final xMsgConnection connection = subscriber.getNewConnection(pubNode);
            final xMsgTopic topic = xMsgTopic.wrap("thr_topic");

            xMsgUtil.sleep(100);

            xMsgSubscription sub = subscriber.subscribe(connection, topic, new xMsgCallBack() {

                @Override
                public xMsgMessage callback(xMsgMessage msg) {
                    xMsgData.Builder data = (xMsgData.Builder) msg.getData();
                    if (data.getBYTES().size() != messageSize) {
                        printf("message of incorrect size received " + data.getBYTES().size());
                        System.exit(1);
                    }
                    int nr = ++timer.nr;
                    if (nr == 1) {
                        timer.watch = ZMQ.zmq_stopwatch_start();
                    } else if (nr == messageCount) {
                        timer.elapsed = ZMQ.zmq_stopwatch_stop(timer.watch);
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

        } catch (SocketException | xMsgException e) {
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
}
