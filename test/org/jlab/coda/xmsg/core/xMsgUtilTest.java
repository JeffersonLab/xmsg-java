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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class xMsgUtilTest {

    @Test
    public void checkValidIPs() throws Exception {
        String[] ips = new String[] {
            "1.1.1.1",
            "255.255.255.255",
            "192.168.1.1",
            "10.10.1.1",
            "132.254.111.10",
            "26.10.2.10",
            "127.0.0.1",
        };

        for (String ip : ips) {
            assertTrue(xMsgUtil.isIP(ip), "isIP: " + ip);
        }
    }

    @Test
    public void checkInvalidIPs() throws Exception {
        String[] ips = new String[] {
            "10.10.10",
            "10.10",
            "10",
            "a.a.a.a",
            "10.10.10.a",
            "10.10.10.256",
            "222.222.2.999",
            "999.10.10.20",
            "2222.22.22.22",
            "22.2222.22.2",
        };

        for (String ip : ips) {
            assertFalse(xMsgUtil.isIP(ip), "is IP: " + ip);
        }
    }

    @Test
    public void checkOnlyIPv4() throws Exception {
        String[] ips = new String[] {
            "2001:cdba:0000:0000:0000:0000:3257:9652",
            "2001:cdba:0:0:0:0:3257:9652",
            "2001:cdba::3257:9652",
        };

        for (String ip : ips) {
            assertFalse(xMsgUtil.isIP(ip), "is IP: " + ip);
        }
    }

    @Test
    public void uniqueReplyToGenerator() throws Exception {
        xMsgUtil.setUniqueReplyToGenerator(0);

        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000000"));
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000001"));
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000002"));

        Thread t1 = new Thread(() -> {
            for (int i = 3; i < 900000; i++) {
                xMsgUtil.getUniqueReplyTo("subject");
            }
        });

        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 90000; i++) {
                xMsgUtil.getUniqueReplyTo("subject");
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1990000"));

        for (int i = 1; i < 10000; i++) {
            xMsgUtil.getUniqueReplyTo("subject");
        }

        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000000"));
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000001"));
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000002"));
    }

    @Test
    public void overflowReplyToGenerator() throws Exception {
        xMsgUtil.setUniqueReplyToGenerator(Integer.MAX_VALUE);

        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1483647"));  // 0x7fff_ffff
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1483648"));  // 0x8000_0000
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1483649"));  // 0x8000_0001

        for (int i = 0; i < 1000000 - 483650; i++) {
            xMsgUtil.getUniqueReplyTo("subject");
        }

        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000000"));
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000001"));
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000002"));

        xMsgUtil.setUniqueReplyToGenerator(-1);

        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1967295"));  // 0xffff_ffff
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000000"));  // 0x0000_0000
        assertThat(xMsgUtil.getUniqueReplyTo("subject"), is("ret:subject:1000001"));  // 0x0000_0001
    }

    @Test
    public void encodeIdentity() throws Exception {
        String encode = xMsgUtil.encodeIdentity(xMsgUtil.localhost(), "test_actor");

        assertThat(encode.length(), is(8));
    }

    @Test
    public void serializeAsBytesAndDeserialize() throws Exception {
        List<String> orig = Arrays.asList("led zeppelin", "pink floyd", "black sabbath");

        byte[] data = xMsgUtil.serializeToBytes(orig);
        @SuppressWarnings("unchecked")
        List<String> clone = (List<String>) xMsgUtil.deserialize(data);

        assertThat(clone, is(orig));
    }

    @Test
    public void serializeAsByteStringAndDeserialize() throws Exception {
        List<String> orig = Arrays.asList("led zeppelin", "pink floyd", "black sabbath");

        ByteString data = xMsgUtil.serializeToByteString(orig);
        @SuppressWarnings("unchecked")
        List<String> clone = (List<String>) xMsgUtil.deserialize(data);

        assertThat(clone, is(orig));
    }
}
