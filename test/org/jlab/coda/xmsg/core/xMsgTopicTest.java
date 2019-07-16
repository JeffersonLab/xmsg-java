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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class xMsgTopicTest {

    @Test
    public void buildWithDomainOnly() throws Exception {
        assertThat(xMsgTopic.build("rock").toString(), is("rock"));

        assertThat(xMsgTopic.build("rock", "*", null).toString(), is("rock"));
        assertThat(xMsgTopic.build("rock", "*", "*").toString(), is("rock"));
    }

    @Test
    public void buildWithSubject() throws Exception {
        assertThat(xMsgTopic.build("rock", "metal").toString(), is("rock:metal"));
    }

    @Test
    public void buildWithTopic() throws Exception {
        assertThat(xMsgTopic.build("rock", "metal", "metallica").toString(),
                   is("rock:metal:metallica"));
    }

    @Test
    public void buildWithExtendedTopic() throws Exception {
        assertThat(xMsgTopic.build("rock", "metal", "metallica:lars:james").toString(),
                   is("rock:metal:metallica:lars:james"));
        assertThat(xMsgTopic.build("rock", "metal", "metallica:lars:*").toString(),
                   is("rock:metal:metallica:lars"));
    }

    @Test
    public void buildWithUndefinedDomainFails() throws Exception {
        for (String s : new String[]{"*", null}) {
            assertThrows(IllegalArgumentException.class, () -> xMsgTopic.build(s));
        }
    }

    @Test
    public void buildWithUndefinedDomainAndValidSubjectFails() throws Exception {
        for (String s : new String[]{"*", null}) {
            assertThrows(IllegalArgumentException.class, () -> xMsgTopic.build(s, "metal"));
        }
    }

    @Test
    public void buildWithNullDomainAndValidTypeFails() throws Exception {
        for (String s : new String[]{"*", null}) {
            assertThrows(IllegalArgumentException.class, () -> xMsgTopic.build(s));
        }
    }

    @Test
    public void buildWithUndefinedSubject() throws Exception {
        assertThat(xMsgTopic.build("rock", null).toString(), is("rock"));
        assertThat(xMsgTopic.build("rock", "*").toString(), is("rock"));

        assertThat(xMsgTopic.build("rock", null, null).toString(), is("rock"));
        assertThat(xMsgTopic.build("rock", null, "*").toString(), is("rock"));
        assertThat(xMsgTopic.build("rock", "*", "*").toString(), is("rock"));
    }

    @Test
    public void buildWithUndefinedType() throws Exception {
        assertThat(xMsgTopic.build("rock", "metal", null).toString(), is("rock:metal"));
        assertThat(xMsgTopic.build("rock", "metal", "*").toString(), is("rock:metal"));
    }

    @Test
    public void wrapWithDomainOnly() throws Exception {
        assertThat(xMsgTopic.wrap("rock").toString(), is("rock"));
    }

    @Test
    public void wrapWithSubject() throws Exception {
        assertThat(xMsgTopic.wrap("rock:metal").toString(), is("rock:metal"));
    }

    @Test
    public void wrapWithTopic() throws Exception {
        assertThat(xMsgTopic.wrap("rock:metal:metallica").toString(),
                   is("rock:metal:metallica"));
    }

    @Test
    public void wrapWithExtendedTopic() throws Exception {
        assertThat(xMsgTopic.wrap("rock:metal:metallica:lars:james").toString(),
                   is("rock:metal:metallica:lars:james"));
    }

    @Test
    public void getDomain() throws Exception {
        assertThat(xMsgTopic.build("rock").domain(), is("rock"));
        assertThat(xMsgTopic.build("rock", "metal").domain(), is("rock"));
        assertThat(xMsgTopic.build("rock", "metal", "metallica").domain(), is("rock"));
        assertThat(xMsgTopic.build("rock", "metal", "metallica:lars").domain(), is("rock"));

        assertThat(xMsgTopic.wrap("rock").domain(), is("rock"));
        assertThat(xMsgTopic.wrap("rock:metal").domain(), is("rock"));
        assertThat(xMsgTopic.wrap("rock:metal:metallica").domain(), is("rock"));
        assertThat(xMsgTopic.wrap("rock:metal:metallica:lars").domain(), is("rock"));
    }

    @Test
    public void getSubject() throws Exception {
        assertThat(xMsgTopic.build("rock").subject(), is(xMsgTopic.ANY));
        assertThat(xMsgTopic.build("rock", "metal").subject(), is("metal"));
        assertThat(xMsgTopic.build("rock", "metal", "metallica").subject(), is("metal"));
        assertThat(xMsgTopic.build("rock", "metal", "metallica:lars").subject(), is("metal"));

        assertThat(xMsgTopic.wrap("rock").subject(), is(xMsgTopic.ANY));
        assertThat(xMsgTopic.wrap("rock:metal").subject(), is("metal"));
        assertThat(xMsgTopic.wrap("rock:metal:metallica").subject(), is("metal"));
        assertThat(xMsgTopic.wrap("rock:metal:metallica:lars").subject(), is("metal"));
    }

    @Test
    public void getType() throws Exception {
        assertThat(xMsgTopic.build("rock").type(), is(xMsgTopic.ANY));
        assertThat(xMsgTopic.build("rock", "metal").type(), is(xMsgTopic.ANY));
        assertThat(xMsgTopic.build("rock", "metal", "metallica").type(), is("metallica"));
        assertThat(xMsgTopic.build("rock", "metal", "metallica:lars").type(), is("metallica:lars"));

        assertThat(xMsgTopic.wrap("rock").type(), is(xMsgTopic.ANY));
        assertThat(xMsgTopic.wrap("rock:metal").type(), is(xMsgTopic.ANY));
        assertThat(xMsgTopic.wrap("rock:metal:metallica").type(), is("metallica"));
        assertThat(xMsgTopic.wrap("rock:metal:metallica:lars").type(), is("metallica:lars"));
    }

    @Test
    public void isParentCompareDomainVersusDomain() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock");

        assertTrue(topic.isParent(xMsgTopic.wrap("rock")));

        assertFalse(topic.isParent(xMsgTopic.wrap("movies")));
    }

    @Test
    public void isParentCompareDomainVersusSubject() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock");

        assertTrue(topic.isParent(xMsgTopic.wrap("rock:metal")));
        assertTrue(topic.isParent(xMsgTopic.wrap("rock:alternative")));

        assertFalse(topic.isParent(xMsgTopic.wrap("movies:thriller")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:classic")));
    }

    @Test
    public void isParentCompareDomainVersusType() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock");

        assertTrue(topic.isParent(xMsgTopic.wrap("rock:metal:metallica")));
        assertTrue(topic.isParent(xMsgTopic.wrap("rock:metal:slayer")));
        assertTrue(topic.isParent(xMsgTopic.wrap("rock:alternative:audioslave")));

        assertFalse(topic.isParent(xMsgTopic.wrap("movies:thriller:shark")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:classic:casablanca")));
    }

    @Test
    public void isParentCompareSubjectVersusDomain() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock:metal");

        assertFalse(topic.isParent(xMsgTopic.wrap("rock")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies")));
    }

    @Test
    public void isParentCompareSubjectVersusSubject() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock:metal");

        assertTrue(topic.isParent(xMsgTopic.wrap("rock:metal")));

        assertFalse(topic.isParent(xMsgTopic.wrap("rock:alternative")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:thriller")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:classic")));
    }

    @Test
    public void isParentCompareSubjectVersusType() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock:metal");

        assertTrue(topic.isParent(xMsgTopic.wrap("rock:metal:metallica")));
        assertTrue(topic.isParent(xMsgTopic.wrap("rock:metal:slayer")));

        assertFalse(topic.isParent(xMsgTopic.wrap("rock:alternative:audioslave")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:thriller:shark")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:classic:casablanca")));
    }

    @Test
    public void isParentCompareTypeVersusDomain() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock:metal:metallica");

        assertFalse(topic.isParent(xMsgTopic.wrap("rock")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies")));
    }

    @Test
    public void isParentCompareTypeVersusSubject() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock:metal:metallica");

        assertFalse(topic.isParent(xMsgTopic.wrap("rock:metal")));
        assertFalse(topic.isParent(xMsgTopic.wrap("rock:alternative")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:thriller")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:classic")));
    }

    @Test
    public void isParentCompareTypeVersusType() throws Exception {
        xMsgTopic topic = xMsgTopic.wrap("rock:metal:metallica");

        assertTrue(topic.isParent(xMsgTopic.wrap("rock:metal:metallica")));

        assertFalse(topic.isParent(xMsgTopic.wrap("rock:metal:slayer")));
        assertFalse(topic.isParent(xMsgTopic.wrap("rock:alternative:audioslave")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:thriller:shark")));
        assertFalse(topic.isParent(xMsgTopic.wrap("movies:classic:casablanca")));
    }
}
