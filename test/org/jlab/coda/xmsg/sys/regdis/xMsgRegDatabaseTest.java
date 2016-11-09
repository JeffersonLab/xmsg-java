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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;


public class xMsgRegDatabaseTest {

    private static final xMsgRegistration.OwnerType TYPE = xMsgRegistration.OwnerType.PUBLISHER;

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
        asimov1 = newRegistration("asimov", "10.2.9.1", "writer:scifi:books");
        asimov2 = newRegistration("asimov", "10.2.9.2", "writer:scifi:books");
        bradbury1 = newRegistration("bradbury", "10.2.9.1", "writer:scifi:books");
        bradbury2 = newRegistration("bradbury", "10.2.9.2", "writer:scifi:books");

        // second test topic, two actors on two hosts
        twain1 = newRegistration("twain", "10.2.9.1", "writer:adventure");
        twain2 = newRegistration("twain", "10.2.9.2", "writer:adventure");

        // third test topic, one actor on second host
        brando2 = newRegistration("brando", "10.2.9.2", "actor");

        // fourth test topic, one actor on first host
        tolkien1 = newRegistration("tolkien", "10.2.9.1", "writer:adventure:tales");
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
        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1)));
    }


    @Test
    public void addNextRegistrationOfFirstTopic() throws Exception {
        db.register(twain1.build());
        db.register(twain2.build());

        assertThat(db.get("writer:adventure"), is(newRegSet(twain1, twain2)));
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

        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1, bradbury1)));
        assertThat(db.get("writer:adventure"), is(newRegSet(twain1)));
        assertThat(db.get("writer:adventure:tales"), is(newRegSet(tolkien1)));
    }


    @Test
    public void addNextRegistrationOfNewTopic() throws Exception {
        db.register(asimov1.build());
        db.register(twain1.build());
        db.register(twain2.build());

        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1)));
        assertThat(db.get("writer:adventure"), is(newRegSet(twain1, twain2)));
    }


    @Test
    public void addDuplicatedRegistrationDoesNothing() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury1.build());
        db.register(bradbury1.build());

        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1, bradbury1)));
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
                   is(newRegSet(asimov1, bradbury1)));
    }


    @Test
    public void removeRegistrationFromTopicWithOneElement() throws Exception {
        db.register(asimov1.build());
        db.register(twain1.build());
        db.register(twain2.build());

        db.remove(asimov1.build());

        assertThat(db.topics(), is(newTopicSet("writer:adventure")));
        assertThat(db.get("writer:adventure"), is(newRegSet(twain1, twain2)));
    }


    @Test
    public void removeRegistrationFromTopicWithSeveralElements() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(twain1.build());
        db.register(twain2.build());

        db.remove(bradbury1.build());

        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1, asimov2)));
        assertThat(db.get("writer:adventure"), is(newRegSet(twain1, twain2)));
    }


    @Test
    public void removeMissingRegistrationDoesNothing() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());

        db.remove(bradbury1.build());

        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1, asimov2)));
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

        assertThat(db.get("writer:scifi:books"), is(newRegSet(asimov1, bradbury1)));
        assertThat(db.get("writer:adventure"), is(newRegSet(twain1)));
        assertThat(db.get("writer:adventure:tales"), is(newRegSet(tolkien1)));
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
                   is(newRegSet(asimov1, twain2, tolkien1)));
        assertThat(db.find("actor", "*", "*"),
                   is(newRegSet(brando2)));
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
                   is(newRegSet(twain1, twain2, tolkien1)));
        assertThat(db.find("actor", "drama", "*"),
                   is(empty()));
    }


    @Test
    public void findByFullTopic() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(tolkien1.build());

        assertThat(db.find("writer", "scifi", "books"),
                   is(newRegSet(asimov1, bradbury2)));
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


    @Test
    public void reverseFindByDomain() throws Exception {
        db.register(asimov1.build());
        db.register(twain2.build());
        db.register(brando2.build());
        db.register(tolkien1.build());

        assertThat(db.rfind("writer", "*", "*"),
                   is(empty()));
        assertThat(db.rfind("actor", "*", "*"),
                   is(newRegSet(brando2)));
    }


    @Test
    public void reverseFindByDomainAndSubject() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(brando2.build());
        db.register(tolkien1.build());

        assertThat(db.rfind("writer", "adventure", "*"), is(newRegSet(twain1, twain2)));
        assertThat(db.rfind("actor", "drama", "*"), is(newRegSet(brando2)));
    }


    @Test
    public void reverseFindByFullTopic() throws Exception {
        db.register(asimov1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(tolkien1.build());

        assertThat(db.rfind("writer", "adventure", "tales"), is(newRegSet(twain1, tolkien1)));
        assertThat(db.rfind("actor", "drama", "movies"), is(newRegSet(brando2)));
    }


    @Test
    public void reverseFindUnregisteredTopicReturnsEmpty() throws Exception {
        db.register(asimov1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        assertThat(db.rfind("writer", "scifi", "tales"), is(empty()));
    }


    @Test
    public void filterByDomain() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        xMsgRegistration.Builder filter = emptyFilter();
        filter.setDomain("writer");

        assertThat(db.filter(filter.build()),
                   is(newRegSet(asimov1, asimov2, bradbury1, bradbury2, twain1, twain2, tolkien1)));
    }


    @Test
    public void filterBySubject() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        xMsgRegistration.Builder filter = emptyFilter();
        filter.setSubject("adventure");

        assertThat(db.filter(filter.build()), is(newRegSet(twain1, twain2, tolkien1)));
    }


    @Test
    public void filterByType() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        xMsgRegistration.Builder filter = emptyFilter();
        filter.setType("books");

        assertThat(db.filter(filter.build()),
                   is(newRegSet(asimov1, asimov2, bradbury1, bradbury2)));
    }


    @Test
    public void filterByAddress() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        xMsgRegistration.Builder filter = emptyFilter();
        filter.setHost("10.2.9.2");

        assertThat(db.filter(filter.build()),
                   is(newRegSet(asimov2, bradbury2, brando2, twain2)));
    }


    @Test
    public void filterUnregisteredTopicReturnsEmpty() throws Exception {
        db.register(asimov1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        xMsgRegistration.Builder filter = emptyFilter();
        filter.setDomain("artist");

        assertThat(db.filter(filter.build()), is(empty()));
    }


    @Test
    public void getSame() throws Exception {
        db.register(asimov1.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        assertThat(db.same("writer", "adventure", "*"), is(newRegSet(twain1, twain2)));
    }


    @Test
    public void getAll() throws Exception {
        db.register(asimov1.build());
        db.register(asimov2.build());
        db.register(bradbury1.build());
        db.register(bradbury2.build());
        db.register(brando2.build());
        db.register(twain1.build());
        db.register(twain2.build());
        db.register(tolkien1.build());

        assertThat(db.all(), is(newRegSet(asimov1, asimov2,
                                          bradbury1, bradbury2,
                                          twain1, twain2,
                                          brando2,
                                          tolkien1
                                          )));
    }


    private static xMsgRegistration.Builder newRegistration(String name,
                                                            String host,
                                                            String topic) {
        return xMsgRegFactory.newRegistration(name, host, TYPE, xMsgTopic.wrap(topic));
    }


    private static xMsgRegistration.Builder emptyFilter() {
        return xMsgRegFactory.newFilter(TYPE);
    }


    private static Set<xMsgTopic> newTopicSet(String... topics) {
        return Stream.of(topics).map(xMsgTopic::wrap).collect(Collectors.toSet());
    }


    private static Set<xMsgRegistration> newRegSet(xMsgRegistration.Builder... regs) {
        return Stream.of(regs).map(r -> r.build()).collect(Collectors.toSet());
    }
}
