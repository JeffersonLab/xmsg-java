/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.coda.xmsg.xsys.regdis;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;

/**
 * A thread that periodically updates xMsg front-end registration database with
 * passed publishers and subscribers database contents. These passed databases
 * (i.e. references) are xMsgNode resident databases, defined within the
 * xMsgRegistrar class: {@link xMsgRegService}
 * <p>
 * This class will be instantiated by the xMsgRegistrar constructor executed by
 * the xMsgNode: {@link org.jlab.coda.xmsg.xsys.xMsgRegistrar} This class
 * inherits from {@link xMsgRegDriver} where xMsg database communication
 * methods are defined.
 * <p>
 * TODO: constructor parameter that defines update periodicity.
 *
 * @author gurjyan
 * @since 1.0
 */
public class xMsgRegUpdater implements Runnable {

    private final xMsgRegDriver driver;
    private final xMsgRegDatabase publishers;
    private final xMsgRegDatabase subscribers;
    private final String name;

    /**
     * Constructor accepts the front-end host name (IP form)
     * as well as two references to publishers and subscribers
     * databases.
     *
     * @param driver the registration driver
     * @param publishers reference to the xMsgNode publishers database
     * @param subscribers reference to the xMsgNode subscribers database
     */
    public xMsgRegUpdater(xMsgRegDriver driver,
                          xMsgRegDatabase publishers,
                          xMsgRegDatabase subscribers) {
        this.driver = driver;
        this.publishers = publishers;
        this.subscribers = subscribers;
        this.name = driver.getAddress() + "_registration_updater";
    }

    @Override
    public void run() {

        while (!Thread.currentThread().isInterrupted()) {

            // update FE publishers database
            for (xMsgTopic key : publishers.topics()) {
                try {
                    for (xMsgRegistration r : publishers.get(key)) {
                        driver.register(r, true);
                        xMsgUtil.sleep(500);
                    }
                } catch (xMsgRegistrationException e) {
                    e.printStackTrace();
                }
            }

            // update FE subscribers database
            for (xMsgTopic key : subscribers.topics()) {
                try {
                    for (xMsgRegistration r : subscribers.get(key)) {
                        driver.register(r, false);
                        xMsgUtil.sleep(500);
                    }
                } catch (xMsgRegistrationException e) {
                    e.printStackTrace();
                }
            }
            xMsgUtil.sleep(5000); // update period 5sec,
        }
    }
}
