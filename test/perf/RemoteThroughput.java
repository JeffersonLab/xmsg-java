package perf;

import java.io.IOException;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

public final class RemoteThroughput {

    private RemoteThroughput() { }

    public static void main(String[] argv) {
        if (argv.length != 3) {
            printf("usage: remote_thr <bind-to> <message-size> <message-count>\n");
            System.exit(1);
        }

        final String bindTo = argv[0];
        final int messageSize = Integer.parseInt(argv[1]);
        final long messageCount = Long.valueOf(argv[2]);

        try {
            final xMsg publisher = new xMsg("thr_publisher", "localhost");
            final xMsgConnection connection = publisher.connect(bindTo);
            final xMsgTopic topic = xMsgTopic.wrap("thr_topic");

            xMsgUtil.sleep(100);


            byte[] data = new byte[messageSize];
            for (int i = 0; i < messageCount; i++) {
                xMsgMessage msg = new xMsgMessage(topic);
                msg.setData("data/binary", data);
                publisher.publish(connection, msg);
            }

            publisher.destroy();

        } catch (IOException | xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void printf(String string) {
        System.out.println(string);
    }
}
