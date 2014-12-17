package org.jlab.coda.xmsg.xsys;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegistrar;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.zeromq.ZContext;


/**
 * <p>
 *     xMsg Front-End
 *     Runs the global registrar service
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgFE {

    // zmq context
    private ZContext context = new ZContext();

    // Thread that runs the registrar service (req/rep server)
    private Thread _registrationThread;

    public static void main(String[] args) {
        final xMsgFE fe = new xMsgFE();

        // Start of the registrar service
        try {
            fe._registrationThread = new xMsgRegistrar(fe.context);
        } catch (xMsgException e) {
            e.printStackTrace();
        }
        fe._registrationThread.start();
        System.out.println(xMsgUtil.currentTime(4) +
                " Info: xMsg FE registration and discovery server is started");

        // Shutdown hook, thread that will run jvm before exiting
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                super.run();
                System.out.println(xMsgUtil.currentTime(4) +
                        " Info: Shutting down xMsg FE registration and discovery server");
                fe._registrationThread.interrupt();
                fe.context.close();
                System.out.println("bye...");
            }
        });

        xMsgUtil.sleep_fe(3000);

    }
}
