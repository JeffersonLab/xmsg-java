package perf;

import java.io.IOException;

import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgD.Data;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;

import com.google.protobuf.ByteString;

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
            final xMsg publisher = new xMsg("localhost");
            final xMsgConnection connection = publisher.connect(bindTo);
            final String topic = "thr_topic";

            xMsgUtil.sleep(100);


            byte[] data = new byte[messageSize];
            for (int i = 0; i < messageCount; i++) {
                Data.Builder builder = Data.newBuilder();
                builder.setBYTES(ByteString.copyFrom(data));
                xMsgMessage msg = new xMsgMessage("thr_publisher", topic, "", "", builder);
                publisher.publish(connection, msg);
            }

            publisher.destroy(5000);

        } catch (IOException | xMsgException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void printf(String string) {
        System.out.println(string);
    }
}
