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

import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.protobuf.ByteString;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class xMsgUtilTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

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
            assertTrue(ip + " should be valid", xMsgUtil.isIP(ip));
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
            assertFalse(ip + " should be invalid", xMsgUtil.isIP(ip));
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
            assertFalse(ip + " should be invalid", xMsgUtil.isIP(ip));
        }
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
