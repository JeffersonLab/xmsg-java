/*
 * Copyright (C) 2015. Jefferson Lab, xMsg framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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
    public static final String FIND_PUBLISHERS_BY_DOMAIN = "findePublishersByDomain";
    public static final String FIND_PUBLISHERS_BY_SUBJECT = "findePublishersBySubject";
    public static final String FIND_SUBSCRIBERS_BY_DOMAIN = "findeSubscribersByDomain";
    public static final String FIND_SUBSCRIBERS_BY_SUBJECT = "findeSubscribersBySubject";
    public static final String RETURN_PUBLISHER_DOMAIN_NAMES = "returnPublisherDomainNames";
    public static final String RETURN_PUBLISHER_SUBJECT_NAMES = "returnPublisherSubjectNames";
    public static final String RETURN_PUBLISHER_TYPE_NAMES = "returnPublisherTypeNames";
    public static final String RETURN_SUBSCRIBER_DOMAIN_NAMES = "returnSubscriberDomainNames";
    public static final String RETURN_SUBSCRIBER_SUBJECT_NAMES = "returnSubscriberSubjectNames";
    public static final String RETURN_SUBSCRIBER_TYPE_NAMES = "returnSubscriberTypeNames";


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

    public static final String SINT32 = "binary/sint32";
    public static final String SINT64 = "binary/sint64";
    public static final String SFIXED32 = "binary/sfixed32";
    public static final String SFIXED64 = "binary/sfixed64";
    public static final String FLOAT = "binary/float";
    public static final String DOUBLE = "binary/double";
    public static final String STRING = "text/string";
    public static final String BYTES = "binary/bytes";

    public static final String ARRAY_SINT32 = "binary/array-sint32";
    public static final String ARRAY_SINT64 = "binary/array-sint64";
    public static final String ARRAY_SFIXED32 = "binary/array-sfixed32";
    public static final String ARRAY_SFIXED64 = "binary/array-sfixed32";
    public static final String ARRAY_FLOAT = "binary/array-float";
    public static final String ARRAY_DOUBLE = "binary/array-double";
    public static final String ARRAY_STRING = "binary/array-string";
    public static final String ARRAY_BYTES = "binary/array-string";


    private xMsgConstants() { }
}
