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

package org.jlab.coda.xmsg.xsys.regdis;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory.newRegistration;


public class xMsgRegDatabaseTest {

    private final xMsgRegistration.Builder asimov1;
    private final xMsgRegistration.Builder bradbury1;
    private final xMsgRegistration.Builder asimov2;
    private final xMsgRegistration.Builder bradbury2;
    private final xMsgRegistration.Builder twain1;
    private final xMsgRegistration.Builder twain2;
    private final xMsgRegistration.Builder brando2;
    private final xMsgRegistration.Builder tolkien1;

    private xMsgRegDatabase db;

    public xMsgRegDatabaseTest() {

        // first test topic, four actors on two hosts
        asimov1 = newRegistration("asimov", "10.2.9.1", "writer:scifi:books", true);
        asimov2 = newRegistration("asimov", "10.2.9.2", "writer:scifi:books", true);
        bradbury1 = newRegistration("bradbury", "10.2.9.1", "writer:scifi:books", true);
        bradbury2 = newRegistration("bradbury", "10.2.9.2", "writer:scifi:books", true);

        // second test topic, two actors on two hosts
        twain1 = newRegistration("twain", "10.2.9.1", "writer:adventure", true);
        twain2 = newRegistration("twain", "10.2.9.2", "writer:adventure", true);

        // third test topic, one actor on second host
        brando2 = newRegistration("brando", "10.2.9.2", "actor", true);

        // fourth test topic, one actor on first host
        tolkien1 = newRegistration("tolkien", "10.2.9.1", "writer:adventure:tales", true);
    }

    private static Set<xMsgTopic> newTopicSet(String... topics) {
        Set<xMsgTopic> set = new HashSet<xMsgTopic>();
        for (String s : topics) {
            set.add(xMsgTopic.wrap(s));
        }
        return set;
    }

    private static Set<xMsgRegistration> newRegSet(xMsgRegistration... regs) {
        Set<xMsgRegistration> set = new HashSet<xMsgRegistration>();
        for (xMsgRegistration r : regs) {
            set.add(r);
        }
        return set;
    }

    @Before
    public void setup() {
        db = new xMsgRegDatabase();
    }

    @Test
    public void newRegistrationDatabaseIsEmpty() throws Exception {
        assertThat(db.topics(), is(empty()));
    }

    @Test
    public void addFirstRegistrationOfFirstTopicCreatesTopic() throws Exception {
        db.register(asimov1.build());

        assertThat(db.topics(), is(newTopicSet("writer:scifi:books")));
        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1.build())));
    }

    @Test
    public void addNextRegistrationOfFirstTopic() throws Exception {
        db.register(twain1.build());
        db.register(twain2.build());

        assertThat(db.get("writer:adventure"),
                   is(newRegSet(twain1.build(), twain2.build())));
    }

    @Test
    public void addFirstRegistrationOfNewTopic() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury1.build());
        db.register(twain1.build());
        db.register(tolkien1.build());

        assertThat(db.topics(),
                is(newTopicSet("writer:scifi:books",
                               "writer:adventure",
                               "writer:adventure:tales")));

        assertThat(db.get("writer:scifi:books"),
                   is(newRegSet(asimov1.build(), bradbury1.build())));

        assertThat(db.get("writer:adventure"),
                   is(newRegSet(twain1.build())));

        assertThat(db.get("writer:adventure:tales"),
                   is(newRegSet(tolkien1.build())));
    }

    @Test
    public void addNextRegistrationOfNewTopic() throws Exception {
        db.register(asimov1.build());
        db.register(twain1.build());
        db.register(twain2.build());

        assertThat(db.get("writer:scifi:books"),
                   is(newRegSet(asimov1.build())));
        assertThat(db.get("writer:adventure"),
                   is(newRegSet(twain1.build(), twain2.build())));
    }

    @Test
    public void addDuplicatedRegistrationDoesNothing() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury1.build());
        db.register(bradbury1.build());

        assertThat(db.get("writer:scifi:books"),
                   is(newRegSet(asimov1.build(), bradbury1.build())));
    }

    @Test
    public void removeRegistrationFromOnlyTopicWithOneElement() throws Exception {
        db.register(asimov1.build());
        db.remove(asimov1.build());

        assertThat(db.find("writer", "scifi", "books"), is(empty()));
    }

    @Test
    public void removeRegistrationFromOnlyTopicWithSeveralElements() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());

        db.remove(asimov2.build());

        assertThat(db.find("writer", "scifi", "books"),
                   is(newRegSet(asimov1.build(), bradbury1.build())));
    }

    @Test
    public void removeRegistrationFromTopicWithOneElement() throws Exception {
        db.register(asimov1.build());
        db.register(twain1.build());
        db.register(twain2.build());

        db.remove(asimov1.build());

        assertThat(db.topics(), is(newTopicSet("writer:adventure")));
        assertThat(db.get("writer:adventure"),
                   is(newRegSet(twain1.build(), twain2.build())));
    }

    @Test
    public void removeRegistrationFromTopicWithSeveralElements() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(twain1.build());
        db.register(twain2.build());

        db.remove(bradbury1.build());

        assertThat(db.get("writer:scifi:books"),
                   is(newRegSet(asimov1.build(), asimov2.build())));
        assertThat(db.get("writer:adventure"),
                   is(newRegSet(twain1.build(), twain2.build())));
    }

    @Test
    public void removeMissingRegistrationDoesNothing() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());

        db.remove(bradbury1.build());

        assertThat(db.get("writer:scifi:books"),
                   is(newRegSet(asimov1.build(), asimov2.build())));
    }

    @Test
    public void removeRegistrationByHost() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(bradbury2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(brando2.build());
        db.register(tolkien1.build());

        db.remove("10.2.9.2");

        assertThat(db.topics(),
                   is(newTopicSet("writer:scifi:books",
                                  "writer:adventure",
                                  "writer:adventure:tales")));

        assertThat(db.get("writer:scifi:books"),
                   is(newRegSet(asimov1.build(), bradbury1.build())));
        assertThat(db.get("writer:adventure"),
                   is(newRegSet(twain1.build())));
        assertThat(db.get("writer:adventure:tales"),
                   is(newRegSet(tolkien1.build())));
    }

    @Test
    public void removeLastRegistrationByData() throws Exception {
        db.register(asimov1.build());

        db.remove(asimov1.build());

        assertThat(db.topics(), is(empty()));
    }

    @Test
    public void removeLastRegistrationByHost() throws Exception {
        db.register(asimov1.build());

        db.remove("10.2.9.1");

        assertThat(db.topics(), is(empty()));
    }

    @Test
    public void findByDomain() throws Exception {
        db.register(asimov1.build());
        db.register(twain2.build());
        db.register(brando2.build());
        db.register(tolkien1.build());

        assertThat(db.find("writer", "*", "*"),
                   is(newRegSet(asimov1.build(), twain2.build(), tolkien1.build())));
        assertThat(db.find("actor", "*", "*"),
                   is(newRegSet(brando2.build())));
    }

    @Test
    public void findByDomainAndSubject() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(brando2.build());
        db.register(tolkien1.build());

        assertThat(db.find("writer", "adventure", "*"),
                   is(newRegSet(twain1.build(), twain2.build(), tolkien1.build())));
    }

    @Test
    public void findByFullTopic() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(tolkien1.build());

        assertThat(db.find("writer", "scifi", "books"),
                   is(newRegSet(asimov1.build(), bradbury2.build())));
        assertThat(db.find("actor", "drama", "movies"),
                   is(empty()));
    }

    @Test
    public void findUnregisteredTopicReturnsEmpty() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(tolkien1.build());

        assertThat(db.find("writer", "adventure", "books"), is(empty()));
    }
}
