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
 * @author gurjyan
 * @since 2.x
 */
public final class xMsgConstants {

    public static final String UNDEFINED = "undefined";
    public static final String SUCCESS = "success";
    public static final String ANY = "*";

    public static final String TOPIC_SEP = ":";
    public static final String DATA_SEP = "?";
    public static final String LANG_SEP = "_";
    public static final String PRXHOSTPORT_SEP = "%";

    public static final String CTRL_TOPIC = "xmsg:control";
    public static final String CTRL_CONNECT = "pub";
    public static final String CTRL_SUBSCRIBE = "sub";
    public static final String CTRL_REPLY = "rep";

    public static final String REGISTRAR = "xMsg_Registrar";

    public static final String REGISTER_PUBLISHER = "registerPublisher";
    public static final String REGISTER_SUBSCRIBER = "registerSubscriber";
    public static final int REGISTER_REQUEST_TIMEOUT = 3000;

    public static final String REMOVE_PUBLISHER = "removePublisherRegistration";
    public static final String REMOVE_SUBSCRIBER = "removeSubscriberRegistration";
    public static final String REMOVE_ALL_REGISTRATION = "removeAllRegistration";
    public static final int REMOVE_REQUEST_TIMEOUT = 3000;

    public static final int FIND_REQUEST_TIMEOUT = 3000;
    public static final String FIND_PUBLISHER = "findPublisher";
    public static final String FIND_SUBSCRIBER = "findSubscriber";

    public static final int FILTER_REQUEST_TIMEOUT = 3000;
    public static final String FILTER_PUBLISHER = "filterPublisher";
    public static final String FILTER_SUBSCRIBER = "filterSubscriber";

    public static final String ALL_PUBLISHER = "allPublisher";
    public static final String ALL_SUBSCRIBER = "allSubscriber";

    public static final String INFO = "INFO";
    public static final String WARNING = "WARNING";
    public static final String ERROR = "ERROR";
    public static final String DONE = "done";
    public static final String DATA = "data";

    public static final String NO_RESULT = "none";

    public static final String BIND = "bind";
    public static final String CONNECT = "connect";

    public static final int DEFAULT_PORT = 7771;
    public static final int REGISTRAR_PORT = 8888;

    public static final int DEFAULT_POOL_SIZE = 2;

    private xMsgConstants() { }
}
