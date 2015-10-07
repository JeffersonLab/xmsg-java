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

/**
 * xMsg is a lightweight publish/subscribe messaging system, presenting asynchronous
 * publish/subscribe inter-process communication environment: simple API layer in Java,
 * Python and C++. xMsg also includes synchronous routines for convenience.
 * <p>
 * xMsg also provides in memory registration database that is used to register xMsg actors.
 * So, xMsg API includes methods for registering and discovering publishers and subscribers.
 * This makes xMsg a suitable framework to build symmetric SOA based applications.
 * For example a services that has a message to publishing can check to see if there
 * are enough subscribers of this type of message.
 * <p>
 * To solve dynamic discovery problem in pub/sub environment the need of a server is
 * unavoidable. xMsg is using 0MQ socket libraries and also borrows 0MQ proxy, which is
 * a simple stateless message switch to address mentioned dynamic discovery problem.
 * <p>
 * xMsg publisher can send a message of any subject. xMsg subscribers subscribe to abstract
 * subjects and provide callbacks to handle messages as they arrive, in a so called
 * subscribe-and-forget mode. Neither publisher nor subscriber knows of each others
 * existence. Thus publishers and subscribers are completely independent of each others.
 * Yet, for a proper communication they need to establish some kind of relationship or
 * binding, and that binding is the communication or message subject. Note that multiple
 * xMsg actors can communicate without interfering with each other via simple {@code "subject"}
 * naming conventions. xMsg subject convention defines 3 part subject: domain, subject, type.
 * presented by the {@link org.jlab.coda.xmsg.core.xMsgTopic} class.
 * <p>
 * xMsg subscriber user provided callbacks {@link org.jlab.coda.xmsg.core.xMsgCallBack} will
 * run in a separate thread. For that reason xNsg provides a thread pool, simplifying the
 * job of a user. Note that user provided callback routines must be thread safe and/or enabled.
 * <p>
 *  In conclusion below we present the xMsg entire API
 *  <ul>
 *      <li> overloaded {@link org.jlab.coda.xmsg.core.xMsg#connect(org.jlab.coda.xmsg.net.xMsgProxyAddress, org.jlab.coda.xmsg.net.xMsgConnectionOption)}</li>
 *      <li> {@link org.jlab.coda.xmsg.core.xMsg#release(org.jlab.coda.xmsg.net.xMsgConnection)}</li>
 *      <li> {@link org.jlab.coda.xmsg.core.xMsg#publish(org.jlab.coda.xmsg.net.xMsgConnection, org.jlab.coda.xmsg.core.xMsgMessage)}</li>
 *      <li> {@link org.jlab.coda.xmsg.core.xMsg#subscribe(org.jlab.coda.xmsg.net.xMsgConnection, org.jlab.coda.xmsg.core.xMsgTopic, org.jlab.coda.xmsg.core.xMsgCallBack)}</li>
 *      <li> {@link org.jlab.coda.xmsg.core.xMsg#unsubscribe(org.jlab.coda.xmsg.core.xMsgSubscription)}</li>
 *      <li> overloaded {@link org.jlab.coda.xmsg.core.xMsg#registerAsPublisher(org.jlab.coda.xmsg.net.xMsgRegAddress, org.jlab.coda.xmsg.core.xMsgTopic, java.lang.String)}</li>
 *      <li> overloaded {@link org.jlab.coda.xmsg.core.xMsg#registerAsSubscriber(org.jlab.coda.xmsg.net.xMsgRegAddress, org.jlab.coda.xmsg.core.xMsgTopic, java.lang.String)}</li>
 *      <li> overloaded {@link org.jlab.coda.xmsg.core.xMsg#removePublisherRegistration(org.jlab.coda.xmsg.net.xMsgRegAddress, org.jlab.coda.xmsg.core.xMsgTopic)}</li>
 *      <li> overloaded {@link org.jlab.coda.xmsg.core.xMsg#removeSubscriberRegistration(org.jlab.coda.xmsg.net.xMsgRegAddress, org.jlab.coda.xmsg.core.xMsgTopic)}</li>
 *      <li> overloaded {@link org.jlab.coda.xmsg.core.xMsg#findPublishers(org.jlab.coda.xmsg.net.xMsgRegAddress, org.jlab.coda.xmsg.core.xMsgTopic)}</li>
 *      <li> overloaded {@link org.jlab.coda.xmsg.core.xMsg#findSubscribers(org.jlab.coda.xmsg.net.xMsgRegAddress, org.jlab.coda.xmsg.core.xMsgTopic)}</li>
 *  </ul>
 *
 * Enjoy...
 *
 *
 * @author gurjyan
 * @since 2.3
 */
package org.jlab.coda.xmsg;