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

package org.jlab.coda.xmsg.xsys.regdis;

import java.util.Arrays;

import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMsg;

import com.google.protobuf.InvalidProtocolBufferException;

import static org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory.newRegistration;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class xMsgRegRequestTest {

    private xMsgRegistration.Builder data;

    @Before
    public void setup() {
        data = newRegistration("asimov", "10.2.9.1", "writer.scifi:books", true);
    }


    @Test
    public void createDataRequest() throws Exception {
        xMsgRegRequest sendRequest = new xMsgRegRequest("foo:bar", "foo_service", data.build());
        xMsgRegRequest recvRequest = new xMsgRegRequest(sendRequest.msg());

        assertThat(recvRequest.topic(), is("foo:bar"));
        assertThat(recvRequest.sender(), is("foo_service"));
        assertThat(recvRequest.data(), is(data.build()));
    }


    @Test
    public void createTextRequest() throws Exception {
        xMsgRegRequest sendRequest = new xMsgRegRequest("foo:bar", "foo_service", "10.2.9.2");
        xMsgRegRequest recvRequest = new xMsgRegRequest(sendRequest.msg());

        assertThat(recvRequest.topic(), is("foo:bar"));
        assertThat(recvRequest.sender(), is("foo_service"));
        assertThat(recvRequest.text(), is("10.2.9.2"));
    }


    @Test(expected = xMsgRegistrationException.class)
    public void failWithMalformedMessage() throws Exception {
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");

        new xMsgRegRequest(msg);
    }


    @Test(expected = InvalidProtocolBufferException.class)
    public void failWithMalformedData() throws Exception {
        byte[] bb = data.build().toByteArray();
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");
        msg.add(Arrays.copyOf(bb, bb.length - 10));

        xMsgRegRequest recvRequest = new xMsgRegRequest(msg);

        recvRequest.data();
    }
}