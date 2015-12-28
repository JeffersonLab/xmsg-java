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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;

import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
import org.junit.Test;

public class xMsgMessageTest {

    private final xMsgTopic testTopic = xMsgTopic.wrap("test_topic");

    @Test
    public void createFromPrimitive() throws Exception {
        xMsgMessage msg;
        xMsgData data;

        msg = xMsgMessage.createFrom(testTopic, 460);
        data = xMsgData.parseFrom(msg.getData());
        assertThat(data.getFLSINT32(), is(460));

        msg = xMsgMessage.createFrom(testTopic, 2000.5);
        data = xMsgData.parseFrom(msg.getData());
        assertThat(data.getDOUBLE(), is(2000.5));

        msg = xMsgMessage.createFrom(testTopic, "test_data");
        data = xMsgData.parseFrom(msg.getData());
        assertThat(data.getSTRING(), is("test_data"));
    }

    @Test
    public void createFromJavaObject() throws Exception {
        List<String> orig = Arrays.asList("led zeppelin", "pink floyd", "black sabbath");

        xMsgMessage msg = xMsgMessage.createFrom(testTopic, orig);
        assertThat(msg.getData(), is(xMsgUtil.serializeToBytes(orig)));
    }
}
