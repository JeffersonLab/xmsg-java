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
import org.zeromq.ZMQ.Context;


/**
 * A wrapper over a 0MQ context to handle connection sockets.
 * <p>
 * A global singleton can be obtained with {@link #getInstance()}.
 * This context is shared by all xMsg actors in the same JVM process,
 * and it must be destroyed at the end of the process,
 * after all actors have been destroyed.
 * <p>
 * New contexts can be created with {@link #newContext()},
 * for cases when the global context cannot be used
 * (i.e. the context should be destroyed before exiting the application)
 *
 * @since 2.x
 */
public final class xMsgContext implements AutoCloseable {

    private static final xMsgContext ourInstance = new xMsgContext();  // nocheck: ConstantName

    // xMsg uses ZMQ.Context for all context operations, but some jeromq APIs
    // may require a shadow ZContext and since 0.4.x a shadow context cannot be
    // created from an existing ZMQ.Context anymore, so this needs to be a
    // ZContext.
    private final ZContext ctx;

    private xMsgContext() {
        ctx = new ZContext(1);
    }

    /**
     * Returns the global singleton context.
     *
     * @return the global xMsg context
     */
    public static xMsgContext getInstance() {
        return ourInstance;
    }

    /**
     * Creates a new xMsg context.
     *
     * @return the created xMsg context
     */
    public static xMsgContext newContext() {
        return new xMsgContext();
    }

    /**
     * Gets the size of the 0MQ thread pool to handle I/O operations.
     *
     * @return the number of I/O threads used by the context
     */
    public int getIOThreads() {
        return ctx.getContext().getIOThreads();
    }

    /**
     * Set the size of the 0MQ thread pool to handle I/O operations.
     *
     * @param ioThreads the number of I/O threads
     */
    public void setIOThreads(int ioThreads) {
        ctx.getContext().setIOThreads(ioThreads);
    }

    /**
     * Gets the maximum number of sockets allowed on the context.
     *
     * @return the maximum number of sockets that can be created
     *         with the context
     */
    public int getMaxSockets() {
        return ctx.getContext().getMaxSockets();
    }

    /**
     * Sets the maximum number of sockets allowed on the context.
     *
     * @param maxSockets the maximum number of sockets that can be created
     *        with the context
     */
    public void setMaxSockets(int maxSockets) {
        ctx.getContext().setMaxSockets(maxSockets);
    }

    /**
     * Returns the internal wrapped 0MQ context.
     *
     * @return the wrapped 0MQ context
     */
    public Context getContext() {
        return ctx.getContext();
    }

    /**
     * Destroys the context.
     * All connections must be already closed otherwise this will hang.
     */
    public void destroy() {
        ctx.destroy();
    }

    /**
     * Destroys the context.
     * All connections must be already closed otherwise this will hang.
     */
    @Override
    public void close() {
        destroy();
    }
}
