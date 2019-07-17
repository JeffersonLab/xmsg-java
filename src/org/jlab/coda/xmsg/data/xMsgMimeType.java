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

package org.jlab.coda.xmsg.data;

import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.data.xMsgD.xMsgData;

/**
 * Predefined types supported by xMsg.
 * <p>
 * Primitive and arrays of primitives will be stored inside a
 * {@link xMsgMessage} as part of a protocol buffers {@link xMsgData} object.
 * Language specific types will be serialized and stored as a byte-array
 * (serialization for these types only work on actors of the same language).
 *
 * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding">
 *      Protocol Buffers Encoding</a>
 */
public final class xMsgMimeType {

    /**
     * A signed integer of 32 bytes and variable length.
     */
    public static final String SINT32 = "binary/sint32";

    /**
     * A signed integer of 64 bytes and variable length.
     */
    public static final String SINT64 = "binary/sint64";

    /**
     * A signed integer of 32 bytes and fixed length.
     */
    public static final String SFIXED32 = "binary/sfixed32";

    /**
     * A signed integer of 32 bytes and fixed length.
     */
    public static final String SFIXED64 = "binary/sfixed64";

    /**
     * A 32 bytes single-precision floating-point number.
     */
    public static final String FLOAT = "binary/float";

    /**
     * A 64 bytes single-precision floating-point number.
     */
    public static final String DOUBLE = "binary/double";

    /**
     * A string.
     */
    public static final String STRING = "text/string";

    /**
     * Binary data.
     */
    public static final String BYTES = "binary/bytes";


    /**
     * An array of signed integers of 32 bytes and variable length.
     */
    public static final String ARRAY_SINT32 = "binary/array-sint32";

    /**
     * An array of signed integers of 64 bytes and variable length.
     */
    public static final String ARRAY_SINT64 = "binary/array-sint64";

    /**
     * An array of signed integers of 32 bytes and fixed length.
     */
    public static final String ARRAY_SFIXED32 = "binary/array-sfixed32";

    /**
     * An array of signed integers of 32 bytes and fixed length.
     */
    public static final String ARRAY_SFIXED64 = "binary/array-sfixed64";

    /**
     * An array of 32 bytes single-precision floating-point numbers.
     */
    public static final String ARRAY_FLOAT = "binary/array-float";

    /**
     * An array of 64 bytes single-precision floating-point numbers.
     */
    public static final String ARRAY_DOUBLE = "binary/array-double";

    /**
     * An array of strings.
     */
    public static final String ARRAY_STRING = "binary/array-string";

    /**
     * A array of binary data.
     */
    public static final String ARRAY_BYTES = "binary/array-bytes";

    /**
     * A {@link xMsgData} object.
     */
    public static final String XMSG_DATA = "binary/native";

    /**
     * A Java object.
     */
    public static final String JOBJECT = "binary/java";

    /**
     * A C++ object.
     */
    public static final String COBJECT = "binary/cpp";

    /**
     * A Python object.
     */
    public static final String POBJECT = "binary/python";

    private xMsgMimeType() { }
}
