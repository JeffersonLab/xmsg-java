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

import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.jlab.coda.xmsg.sys.regdis.RegistrationDataFactory.newRegistration;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class xMsgRegResponseTest {

    private xMsgRegistration.Builder data1;
    private xMsgRegistration.Builder data2;

    @BeforeEach
    public void setup() {
        xMsgRegistration.OwnerType type = xMsgRegistration.OwnerType.SUBSCRIBER;
        data1 = newRegistration("asimov", "10.2.9.1", type, "writer.scifi:books");
        data2 = newRegistration("bradbury", "10.2.9.1", type, "writer.scifi:books");
    }


    @Test
    public void createSuccessResponse() throws Exception {
        xMsgRegResponse sendResponse = new xMsgRegResponse("foo:bar", "registration_fe");
        xMsgRegResponse recvResponse = new xMsgRegResponse(sendResponse.msg());

        assertThat(recvResponse.topic(), is("foo:bar"));
        assertThat(recvResponse.sender(), is("registration_fe"));
        assertThat(recvResponse.status(), is(xMsgRegConstants.SUCCESS));
        assertThat(recvResponse.data(), is(empty()));
    }


    @Test
    public void createErrorResponse() throws Exception {
        String error = "could not handle request";
        xMsgRegResponse sendResponse = new xMsgRegResponse("foo:bar", "registration_fe", error);

        xMsgException ex = assertThrows(xMsgException.class, () ->
                new xMsgRegResponse(sendResponse.msg()));
        assertThat(ex.getMessage(), containsString(error));
    }


    @Test
    public void createDataResponse() throws Exception {
        Set<xMsgRegistration> data = new HashSet<>(Arrays.asList(data1.build(), data2.build()));
        xMsgRegResponse sendResponse = new xMsgRegResponse("foo:bar", "registration_fe", data);
        xMsgRegResponse recvResponse = new xMsgRegResponse(sendResponse.msg());

        assertThat(recvResponse.topic(), is("foo:bar"));
        assertThat(recvResponse.sender(), is("registration_fe"));
        assertThat(recvResponse.status(), is(xMsgRegConstants.SUCCESS));
        assertThat(recvResponse.data(), is(data));
    }


    @Test
    public void failWithMalformedMessage() throws Exception {
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");

        xMsgException ex = assertThrows(xMsgException.class, () -> new xMsgRegResponse(msg));
        assertThat(ex.getMessage(), is("invalid registrar server response format"));
    }


    @Test
    public void failWithMalformedData() throws Exception {
        byte[] bb = data1.build().toByteArray();
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");
        msg.addString(xMsgRegConstants.SUCCESS);
        msg.add(data2.build().toByteArray());
        msg.add(Arrays.copyOf(bb, bb.length - 10));

        xMsgException ex = assertThrows(xMsgException.class, () -> new xMsgRegResponse(msg));
        assertThat(ex.getMessage(), is("could not parse registrar server response"));
    }
}
