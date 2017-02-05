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

package org.jlab.coda.xmsg.core;

/**
 * xMsg constants.
 *
 * @since 2.x
 */
public final class xMsgConstants {

    /** The regex indicating any topic part. */
    public static final String ANY = "*";

    /** The character to separate the topic parts. */
    public static final String TOPIC_SEP = ":";

    /** The default proxy server port. */
    public static final int DEFAULT_PORT = 7771;

    /** The default registrar server port. */
    public static final int REGISTRAR_PORT = 8888;

    /** The default size for the callback thread pool. */
    public static final int DEFAULT_POOL_SIZE = 2;

    /** The default timeout to wait for a connection confirmation. */
    public static final int CONNECTION_TIMEOUT = 1000;

    /** The default timeout to wait for a subscription confirmation. */
    public static final int SUBSCRIPTION_TIMEOUT = 1000;

    /** The default timeout to wait for a registration request response. */
    public static final int REGISTRATION_TIMEOUT = 3000;

    /** The default timeout to wait for a discovery request response. */
    public static final int DISCOVERY_TIMEOUT = 3000;

    private xMsgConstants() { }
}
