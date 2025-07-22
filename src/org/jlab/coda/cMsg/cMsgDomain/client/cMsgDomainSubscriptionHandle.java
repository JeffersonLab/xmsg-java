/*----------------------------------------------------------------------------*
 *  Copyright (c) 2009        Jefferson Science Associates,                   *
 *                            Thomas Jefferson National Accelerator Facility  *
 *                                                                            *
 *    This software was developed under a United States Government license    *
 *    described in the NOTICE file included as part of this distribution.     *
 *                                                                            *
 *    C. Timmer, 6-Feb-2009, Jefferson Lab                                    *
 *                                                                            *
 *     Author: Carl Timmer                                                    *
 *             timmer@jlab.org                   Jefferson Lab, MS-12B3       *
 *             Phone: (757) 269-5130             12000 Jefferson Ave.         *
 *             Fax:   (757) 269-6248             Newport News, VA 23606       *
 *                                                                            *
 *----------------------------------------------------------------------------*/

package org.jlab.coda.cMsg;

import org.jlab.coda.cMsg.cMsgSubscriptionHandle;
import org.jlab.coda.cMsg.cMsgCallbackInterface;
import org.jlab.coda.cMsg.common.cMsgMessageContextDefault;
import org.jlab.coda.cMsg.cMsgConstants;
import org.jlab.coda.xmsg.core.xMsgSubscription;
import org.jlab.coda.xmsg.core.xMsg;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to run a message callback in its own thread.
 * The thread is self-starting and waits to execute the callback.
 * All it needs is a notify.
 */
public class cMsgDomainSubscriptionHandle implements cMsgSubscriptionHandle {

    /** xMsg actor*/
    xMsg xMsgActor;

    /** xMsgSubscription handler */
    xMsgSubscription xMsgHandle;

    /** Subscription's domain. */
    String domain;

    /** Subscription's subject. */
    String subject;

    /** Subscription's type. */
    String type;

    /** User argument to be passed to the callback. */
    private Object arg;

    /** Callback to be run. */
    cMsgCallbackInterface callback;


    /**
     * Stops the background subscription thread and unsubscribes the socket.
     *
     * @param [NOT USED] callInterrupt if true interrupt is called on callback thread,
     *                      else interrupt is not called.
     */
    public void dieNow(boolean callInterrupt) {
      this.xMsgActor.unsubscribe(this.xMsgHandle);
    }

    /**
     * Class to return info on callback's running environment to the callback.
     * In this case we tell callback the cue size.
     */
    private class myContext extends cMsgMessageContextDefault {
        public String getDomain()  { return domain; }
        public String getSubject() { return subject; }
        public String getType()    { return type; }
        public int getQueueSize()  { return 2; }
    }

    /** Object that tells callback user the context info including the cue size. */
    private myContext context;

    /**
     * [NO MORE WORKING]
     * This method stops any further calling of the callback. Any threads currently running
     * the callback continue normally. Messages are still being delivered to this callback's
     * queue.
     */
    public void pause() {
       System.err.println("cMsgSubscriptionHandle::pause() does not work anymore");
    }

    /**
     * [NO MORE WORKING]
     * This method (re)starts any calling of the callback delayed by the {@link #pause} method.
     * Would like to call this method "resume", but that name is taken by the "Thread" class.
     */
    public void restart() {
      System.err.println("cMsgSubscriptionHandle::pause() does not work anymore");
    }

    /**
     * [NOT WORKING ANYMORE, GIVE ONLY A HARDCODE VALUE 2]
     * Gets the number of messages passed to the callback.
     * @return number of messages passed to the callback
     */
    public long getMsgCount() {
        System.err.println("cMsgSubscriptionHandle::getMsgCount() does not work anymore");
        return 2;
    }

    /**
     * Gets the domain in which this subscription lives.
     * @return the domain in which this subscription lives
     */
    public String getDomain()  { return domain; }

    /**
     * Gets the subject of this subscription.
     * @return the subject of this subscription
     */
    public String getSubject() { return subject; }

    /**
     * Gets the type of this subscription.
     * @return the type of this subscription
     */
    public String getType()    { return type; }

    /**
     * [NOT WORKING ANYMORE, GIVE ONLY A HARDCODE VALUE 2]
     * Gets the number of messages in the queue.
     * @return number of messages in the queue
     */
    public int getQueueSize() {
        System.err.println("cMsgSubscriptionHandle::getQueueSize() does not work anymore");
        return 2;
    }

    /**
     * [NOT WORKING ANYMORE, GIVE ONLY A HARDCODE VALUE false]
     * Returns true if queue is full.
     * @return true if queue is full
     */
    public boolean isQueueFull() {
        System.err.println("cMsgSubscriptionHandle::isQueueFull() does not work anymore");
        return false;
    }

    /**
     * [NOT WORKING ANYMORE]
     * Clears the queue of all messages.
     */
    public void clearQueue() {
        System.err.println("cMsgSubscriptionHandle::clearQueue() does not work anymore");
    }

    /**
     * Gets the callback object.
     * @return user callback object
     */
    public cMsgCallbackInterface getCallback() {
        return callback;
    }

    /**
     * Gets the subscription's user object argument.
     * @return subscription's user object argument
     */
    public Object getUserObject() {
        return arg;
    }

    /**
     * [NOT WORKING ANYMORE, GIVE ONLY A HARDCODE VALUE 2]
     * Gets the number of identical subscriptions.
     * @return the number of identical subscriptions
     */
    public int getCount() {
        System.err.println("cMsgSubscriptionHandle::getCount() does not work anymore");
        return 2;
    }

    /**
     * [NOT WORKING ANYMORE]
     * Sets the number of identical subscriptions.
     * @param count the number of identical subscriptions
     */
    public void setCount(int count) {
        System.err.println("cMsgSubscriptionHandle::setCount() does not work anymore");
    }


    /**
     * Constructor.
     *
     * @param callback callback to be run when message arrives
     * @param arg user-supplied argument for callback
     * @param domain subscription's domain
     * @param subject subscription's subject
     * @param type subscription's tyoe
     */
    public cMsgDomainSubscriptionHandle(xMsg xMsgActor, cMsgCallbackInterface callback, Object arg,
                              String domain, String subject, String type) {
        this.xMsgActor = xMsgActor;
        this.callback = callback;
        this.arg      = arg;
        this.domain   = domain;
        this.subject  = subject;
        this.type     = type;
        context       = new myContext();
    }

    public void setXMsgHandle(xMsgSubscription xMsgHandle) {
      this.xMsgHandle = xMsgHandle;
    }

    public xMsgSubscription getXMsgHandle() {
      return this.xMsgHandle;
    }

}
