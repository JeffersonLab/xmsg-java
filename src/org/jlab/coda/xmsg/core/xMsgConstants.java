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

package org.jlab.coda.xmsg.core;

/**
 * xMsg constants.
 *
 * @author gurjyan
 * @since 2.x
 */
public enum xMsgConstants {

    UNDEFINED(20, "undefined"),
    SUCCESS(21, "success"),
    ANY(22, "*"),

    REGISTRAR(3, "xMsg_Registrar"),

    REGISTER_PUBLISHER(4, "registerPublisher"),
    REGISTER_SUBSCRIBER(5, "registerSubscriber"),
    REGISTER_REQUEST_TIMEOUT(3000, "registerRequestTimeout"),

    REMOVE_PUBLISHER(6, "removePublisherRegistration"),
    REMOVE_SUBSCRIBER(7, "removeSubscriberRegistration"),
    REMOVE_ALL_REGISTRATION(8, "removeAllRegistration"),
    REMOVE_REQUEST_TIMEOUT(3000, "removeRequestTimeout"),

    FIND_REQUEST_TIMEOUT(3000, "findRequestTimeout"),
    FIND_PUBLISHER(9, "findPublisher"),
    FIND_SUBSCRIBER(10, "findSubscribers"),

    INFO(11, "INFO"),
    WARNING(12, "WARNING"),
    ERROR(13, "ERROR"),
    DONE(14, "done"),
    DATA(15, "data"),

    NO_RESULT(16, "none"),

    BIND(17, "bind"),
    CONNECT(18, "connect"),

    DEFAULT_PORT(7771, "proxyPort"),
    REGISTRAR_PORT(8888, "registrarPort"),

    DEFAULT_POOL_SIZE(2, "defaultPoolSize"),

    SINT32(0, "binary/sint32"),
    SINT64(0, "binary/sint64"),
    SFIXED32(0, "binary/sfixed32"),
    SFIXED64(0, "binary/sfixed64"),
    FLOAT(0, "binary/float"),
    DOUBLE(0, "binary/double"),
    STRING(0, "text/string"),
    BYTES(0, "binary/bytes"),

    ARRAY_SINT32(0, "binary/array-sint32"),
    ARRAY_SINT64(0, "binary/array-sint64"),
    ARRAY_SFIXED32(0, "binary/array-sfixed32"),
    ARRAY_SFIXED64(0, "binary/array-sfixed32"),
    ARRAY_FLOAT(0, "binary/array-float"),
    ARRAY_DOUBLE(0, "binary/array-double"),
    ARRAY_STRING(0, "binary/array-string"),
    ARRAY_BYTES(0, "binary/array-string");


    private final int intValue;
    private final String stringValue;

    xMsgConstants(int intValue, String stringValue) {
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public String getStringValue() {
        return stringValue;
    }
}
