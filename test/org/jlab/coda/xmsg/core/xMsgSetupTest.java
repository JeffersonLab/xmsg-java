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

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.jlab.coda.xmsg.net.xMsgRegAddress;

public class xMsgSetupTest {

    @Test
    public void defaultValues() throws Exception {
        xMsgSetup setup = xMsgSetup.newBuilder().build();

        assertThat(setup.proxyAddress(), is(new xMsgProxyAddress()));
        assertThat(setup.registrarAddress(), is(new xMsgRegAddress()));
        assertThat(setup.poolSize(), is(xMsgConstants.DEFAULT_POOL_SIZE));
        assertThat(setup.subscriptionMode(), is(xMsgCallbackMode.MULTI_THREAD));
    }


    @Test
    public void customValues() throws Exception {
        xMsgSetup setup = xMsgSetup.newBuilder()
                                   .withProxy(new xMsgProxyAddress("10.1.1.10"))
                                   .withRegistrar(new xMsgRegAddress("10.1.1.1"))
                                   .withPoolSize(5)
                                   .withSubscriptionMode(xMsgCallbackMode.SINGLE_THREAD)
                                   .build();

        assertThat(setup.proxyAddress(), is(new xMsgProxyAddress("10.1.1.10")));
        assertThat(setup.registrarAddress(), is(new xMsgRegAddress("10.1.1.1")));
        assertThat(setup.poolSize(), is(5));
        assertThat(setup.subscriptionMode(), is(xMsgCallbackMode.SINGLE_THREAD));
    }
}
