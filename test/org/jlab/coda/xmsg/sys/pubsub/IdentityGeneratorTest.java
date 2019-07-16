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

package org.jlab.coda.xmsg.sys.pubsub;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class IdentityGeneratorTest {

    @Test
    public void ctrlIdentityHas9Digits() throws Exception {
        assertThat(IdentityGenerator.getCtrlId(), hasLength(9));
        assertThat(IdentityGenerator.getCtrlId(), hasLength(9));
        assertThat(IdentityGenerator.getCtrlId(), hasLength(9));
    }

    @Test
    public void ctrlIdentityPrefixHas3Digits() throws Exception {
        String prefix1 = IdentityGenerator.getCtrlId().substring(1, 4);
        String prefix2 = IdentityGenerator.getCtrlId().substring(1, 4);
        String prefix3 = IdentityGenerator.getCtrlId().substring(1, 4);

        assertThat(prefix1, is(prefix2));
        assertThat(prefix1, is(prefix3));
        assertThat(prefix2, is(prefix3));
    }

    @Test
    public void ctrlIdentityFirstDigitIsJavaIdentifier() throws Exception {
        assertThat(IdentityGenerator.getCtrlId().charAt(0), is('1'));
        assertThat(IdentityGenerator.getCtrlId().charAt(0), is('1'));
        assertThat(IdentityGenerator.getCtrlId().charAt(0), is('1'));
    }

    @Test
    public void ctrlIdentityPrefixLastFiveDigitsAreRandom() throws Exception {
        String suffix1 = IdentityGenerator.getCtrlId().substring(4);
        String suffix2 = IdentityGenerator.getCtrlId().substring(4);
        String suffix3 = IdentityGenerator.getCtrlId().substring(4);

        assertThat(suffix1, is(not(suffix2)));
        assertThat(suffix1, is(not(suffix3)));
        assertThat(suffix2, is(not(suffix3)));
    }
}
