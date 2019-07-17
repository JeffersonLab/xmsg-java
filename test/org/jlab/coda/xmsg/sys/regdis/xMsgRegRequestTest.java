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

package org.jlab.coda.xmsg.sys.regdis;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.ZMsg;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.jlab.coda.xmsg.sys.regdis.RegistrationDataFactory.newRegistration;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class xMsgRegRequestTest {

    private xMsgRegistration.Builder data;

    @BeforeEach
    public void setup() {
        xMsgRegistration.OwnerType type = xMsgRegistration.OwnerType.SUBSCRIBER;
        data = newRegistration("asimov", "10.2.9.1", type, "writer.scifi:books");
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


    @Test
    public void failWithMalformedMessage() throws Exception {
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");

        assertThrows(xMsgException.class, () -> new xMsgRegRequest(msg));
    }


    @Test
    public void failWithMalformedData() throws Exception {
        byte[] bb = data.build().toByteArray();
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");
        msg.add(Arrays.copyOf(bb, bb.length - 10));

        xMsgRegRequest recvRequest = new xMsgRegRequest(msg);

        assertThrows(InvalidProtocolBufferException.class, recvRequest::data);
    }
}
