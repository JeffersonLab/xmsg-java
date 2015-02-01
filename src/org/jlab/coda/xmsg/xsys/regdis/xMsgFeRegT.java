package org.jlab.coda.xmsg.xsys.regdis;

import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistrationData;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.core.xMsgUtil;

import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * <p>
 *      This is a thread that periodically updates xMsg front-end
 *      registration database with passed publishers and subscribers
 *      database contents. These passed databases (i.e. references)
 *      are xMsgNode resident databases, defined within the xMsgRegistrar class:
 *      {@link org.jlab.coda.xmsg.xsys.regdis.xMsgRegistrar}
 *
 *      This class will be instantiated by the xMsgRegistrar constructor
 *      executed by the xMsgNode:
 *      {@link org.jlab.coda.xmsg.xsys.xMsgNode}
 *
 *      This class inherits from {@link xMsgRegDiscDriver}
 *      where xMsg database communication methods are defined.
 *
 *      todo: constructor parameter that defines update periodicity
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgFeRegT extends xMsgRegDiscDriver implements Runnable {

    // xMsgNode database references
    private ConcurrentHashMap<String, xMsgRegistrationData> publishers_db;
    private ConcurrentHashMap<String, xMsgRegistrationData> subscribers_db;

    /**
     * <p>
     * Constructor accepts the front-end host name (IP form)
     * as well as two references to publishers and subscribers
     * databases.
     * </p>
     * @param feHost front-end host name
     * @param publishers_db reference to the xMsgNode
     *                      publishers database
     * @param subscribers_db reference to the xMsgNode
     *                       subscribers database
     * @throws xMsgException
     */
    public xMsgFeRegT(String feHost,
                     ConcurrentHashMap<String, xMsgRegistrationData> publishers_db,
                     ConcurrentHashMap<String, xMsgRegistrationData> subscribers_db
    ) throws xMsgException, SocketException {
        super(feHost);
        this.publishers_db = publishers_db;
        this.subscribers_db = subscribers_db;
    }

    @Override
    public void run() {

        while (!Thread.currentThread().isInterrupted()) {

            // update FE publishers database
            for(String key:publishers_db.keySet()){
                try {
                    register_fe(key, publishers_db.get(key), true);
                    xMsgUtil.sleep(500);
                } catch (xMsgRegistrationException e) {
                    e.printStackTrace();
                }
            }

            // update FE subscribers database
            for(String key:subscribers_db.keySet()){
                try {
                    register_fe(key, subscribers_db.get(key), false);
                    xMsgUtil.sleep(500);
                } catch (xMsgRegistrationException e) {
                    e.printStackTrace();
                }
            }
            xMsgUtil.sleep(5000); // update period 5sec,
        }
    }

}