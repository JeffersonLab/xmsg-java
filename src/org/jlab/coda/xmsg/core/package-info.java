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

/**
 * Main classes for xMsg clients.
 * <p>
 * xMsg {@link org.jlab.coda.xmsg.core.xMsg actors} use
 * {@link org.jlab.coda.xmsg.core.xMsgConnection connections} to publish and
 * subscribe {@link org.jlab.coda.xmsg.core.xMsgMessage messages} to specific
 * {@link org.jlab.coda.xmsg.core.xMsgTopic topics}.
 * When subscribing, a {@link org.jlab.coda.xmsg.core.xMsgCallBack callback}
 * must be provided to process the received messages. Each actor process its
 * registered callbacks in a background thread pool.
 * <p>
 * Actors are data-agnostic, i.e., the data is always stored and published in
 * the message as a binary byte array. Serialization and interpretation of the
 * data must be user-provided.
 * <p>
 * A {@link org.jlab.coda.xmsg.sys.xMsgProxy proxy server} must be running and both
 * publisher(s) and subscriber(s) must use a connection to the same proxy for
 * the messages to be delivered.
 * Actors can register with a {@link org.jlab.coda.xmsg.sys.xMsgRegistrar
 * registrar server} to be discovered by other actors.
 */
package org.jlab.coda.xmsg.core;
