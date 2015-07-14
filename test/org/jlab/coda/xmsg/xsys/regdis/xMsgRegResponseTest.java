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
import java.util.HashSet;
import java.util.Set;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.zeromq.ZMsg;

import static org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory.newRegistration;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class xMsgRegResponseTest {

    private xMsgRegistration.Builder data1;
    private xMsgRegistration.Builder data2;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    @Before
    public void setup() {
        data1 = newRegistration("asimov", "10.2.9.1", "writer.scifi:books", true);
        data2 = newRegistration("bradbury", "10.2.9.1", "writer.scifi:books", true);
    }


    @Test
    public void createSuccessResponse() throws Exception {
        xMsgRegResponse sendResponse = new xMsgRegResponse("foo:bar", "registration_fe");
        xMsgRegResponse recvResponse = new xMsgRegResponse(sendResponse.msg());

        assertThat(recvResponse.topic(), is("foo:bar"));
        assertThat(recvResponse.sender(), is("registration_fe"));
        assertThat(recvResponse.status(), is(xMsgConstants.SUCCESS.toString()));
        assertThat(recvResponse.data(), is(empty()));
    }


    @Test
    public void createErrorResponse() throws Exception {
        String error = "could not handle request";
        xMsgRegResponse sendResponse = new xMsgRegResponse("foo:bar", "registration_fe", error);

        expectedEx.expect(xMsgRegistrationException.class);
        expectedEx.expectMessage(error);
        new xMsgRegResponse(sendResponse.msg());
    }


    @Test
    public void createDataResponse() throws Exception {
        Set<xMsgRegistration> data = new HashSet<>(Arrays.asList(data1.build(), data2.build()));
        xMsgRegResponse sendResponse = new xMsgRegResponse("foo:bar", "registration_fe", data);
        xMsgRegResponse recvResponse = new xMsgRegResponse(sendResponse.msg());

        assertThat(recvResponse.topic(), is("foo:bar"));
        assertThat(recvResponse.sender(), is("registration_fe"));
        assertThat(recvResponse.status(), is(xMsgConstants.SUCCESS.toString()));
        assertThat(recvResponse.data(), is(data));
    }


    public void failWithMalformedMessage() throws Exception {
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");

        expectedEx.expect(xMsgRegistrationException.class);
        expectedEx.expectMessage("xMsg message format violation");
        new xMsgRegResponse(msg);
    }


    @Test
    public void failWithMalformedData() throws Exception {
        byte[] bb = data1.build().toByteArray();
        ZMsg msg = new ZMsg();
        msg.addString("foo:bar");
        msg.addString("foo_service");
        msg.addString(xMsgConstants.SUCCESS.toString());
        msg.add(data2.build().toByteArray());
        msg.add(Arrays.copyOf(bb, bb.length - 10));

        expectedEx.expect(xMsgRegistrationException.class);
        expectedEx.expectMessage("Could not deserialize data");
        new xMsgRegResponse(msg);
    }
}