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

package org.jlab.coda.xmsg.net;

import org.zeromq.ZContext;


/**
 * Singleton class that provides unique
 * {@link org.zeromq.ZContext 0MQ contex} for entire JVM process.
 *
 * @since 2.x
 */
public final class xMsgContext {

    private static final xMsgContext ourInstance = new xMsgContext(); // NOT CONSTANT
    private static final Object lock = new Object(); // NOT CONSTANT

    private final ZContext context;

    private xMsgContext() {
        context = new ZContext();
    }

    private static xMsgContext getInstance() {
        return ourInstance;
    }

    /**
     * Returns the global singleton 0MQ context.
     *
     * @return the global 0MQ context
     */
    public static ZContext getContext() {
        return getInstance().context;
    }

    /**
     * Set the size of the 0MQ thread pool to handle I/O operations.
     *
     * @param ioThreads the number of I/O threads
     */
    public static void setIOThreads(int ioThreads) {
        ourInstance.context.getContext().setIOThreads(ioThreads);
    }

    /**
     * Sets the maximum number of sockets allowed on the context.
     *
     * @param maxSockets the maximum number of sockets that can be created
     *        with the context
     */
    public static void setMaxSockets(int maxSockets) {
        ourInstance.context.getContext().setMaxSockets(maxSockets);
    }

    /**
     * Destroys the global singleton 0MQ context.
     * All connections and actors must be closed otherwise this will hang.
     */
    public static void destroyContext() {
        synchronized (lock) {
            ourInstance.context.destroy();
        }
    }
}
