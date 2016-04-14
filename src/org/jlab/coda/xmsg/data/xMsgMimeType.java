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

public final class xMsgMimeType {

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
    public static final String ARRAY_BYTES = "binary/array-bytes";

    public static final String XMSG_DATA = "binary/native";
    public static final String JOBJECT = "binary/java";
    public static final String COBJECT = "binary/cpp";
    public static final String POBJECT = "binary/python";

    private xMsgMimeType() { }
}
