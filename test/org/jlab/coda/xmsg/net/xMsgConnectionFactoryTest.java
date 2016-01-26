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

package org.jlab.coda.xmsg.net;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class xMsgConnectionFactoryTest {

    @Test
    public void ctrlIdentityHas9Digits() throws Exception {
        assertThat(xMsgConnectionFactory.getCtrlId().length(), is(9));
        assertThat(xMsgConnectionFactory.getCtrlId().length(), is(9));
        assertThat(xMsgConnectionFactory.getCtrlId().length(), is(9));
    }

    @Test
    public void ctrlIdentityPrefixHas3Digits() throws Exception {
        String prefix1 = xMsgConnectionFactory.getCtrlId().substring(1, 4);
        String prefix2 = xMsgConnectionFactory.getCtrlId().substring(1, 4);
        String prefix3 = xMsgConnectionFactory.getCtrlId().substring(1, 4);

        assertThat(prefix1, is(prefix2));
        assertThat(prefix1, is(prefix3));
        assertThat(prefix2, is(prefix3));
    }

    @Test
    public void ctrlIdentityFourthDigitIsJavaIdentifier() throws Exception {
        assertThat(xMsgConnectionFactory.getCtrlId().charAt(0), is('1'));
        assertThat(xMsgConnectionFactory.getCtrlId().charAt(0), is('1'));
        assertThat(xMsgConnectionFactory.getCtrlId().charAt(0), is('1'));
    }

    @Test
    public void ctrlIdentityPrefixLastFiveDigitsAreRandom() throws Exception {
        String suffix1 = xMsgConnectionFactory.getCtrlId().substring(4);
        String suffix2 = xMsgConnectionFactory.getCtrlId().substring(4);
        String suffix3 = xMsgConnectionFactory.getCtrlId().substring(4);

        assertThat(suffix1, is(not(suffix2)));
        assertThat(suffix1, is(not(suffix3)));
        assertThat(suffix2, is(not(suffix3)));
    }
}
