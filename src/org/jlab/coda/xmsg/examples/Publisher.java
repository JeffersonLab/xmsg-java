package org.jlab.coda.xmsg.examples;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnection;
import org.jlab.coda.xmsg.core.xMsg;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.core.xMsgUtil;

import java.util.Random;

/**
 * An example of a publisher that publishes
 * data required size in kBytes for ever.
 * He does not care who is subscribing to his
 * data.
 *
 * @author gurjyan
 * @version 1.x
 * @since 11/4/14
 */
public class Publisher extends xMsg {
    private static final String myName = "test_publisher";
    private static final String domain = "test_domain";
    private static final String subject = "test_subject";
    private static final String type = "test_type";

    /**
     * <p>
     * Constructor, requires the name of the FrontEnd host that is used
     * to create a connection to the registrar service running within the
     * xMsgFE. Creates the zmq context object and thread pool for servicing
     * received messages in a separate threads.
     * Localhost is considered to be the host of the FE.
     * Thread pool is relevant for subscribers only.
     * </p>
     *
     * @throws org.jlab.coda.xmsg.excp.xMsgException
     */
    public Publisher() throws xMsgException {
        super("localhost");
    }

    public static void main(String[] args) {
        try {

            Publisher publisher = new Publisher();

            // Create a socket connections to the xMsg node
            xMsgConnection con =  publisher.connect();

            // Register this publisher
            publisher.registerPublisher(myName, domain, subject,type);

            // Create array of integers as a message payload.
            // The only argument defines the payload size.
            int size = Integer.parseInt(args[0]);
            Integer[] data = new Integer[size];

            // Fill payload with random numbers
            Random rg = new Random();
            for(int i=0; i<size; i++) data[i] = rg.nextInt();

            // Create the message to be published
            xMsgMessage msg = new xMsgMessage(myName, domain, subject,type, data);

            // Publish data for ever...
            while(true) {
                publisher.publish(con, msg);
                System.out.println("publishing...");
                xMsgUtil.sleep(1000);
            }

        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }
}
