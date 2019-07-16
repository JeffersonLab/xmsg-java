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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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


    @BeforeEach
    public void setup() {
        db = new xMsgRegDatabase();
    }

    private void register(xMsgRegistration.Builder... regs) {
        Stream.of(regs).forEach(r -> db.register(r.build()));
    }

    private void registerAll() {
        register(asimov1, asimov2, bradbury1, bradbury2, brando2, twain1, twain2, tolkien1);
    }

    private void remove(xMsgRegistration.Builder... regs) {
        Stream.of(regs).forEach(r -> db.remove(r.build()));
    }


    @Test
    public void newRegistrationDatabaseIsEmpty() throws Exception {
        assertThat(db.topics(), is(empty()));
    }


    @Test
    public void addFirstRegistrationOfFirstTopicCreatesTopic() throws Exception {
        register(asimov1);

        assertThat(db.topics(), is(setOf("writer:scifi:books")));
        assertThat(db.get("writer:scifi:books"), is(setOf(asimov1)));
    }


    @Test
    public void addNextRegistrationOfFirstTopic() throws Exception {
        register(twain1, twain2);

        assertThat(db.get("writer:adventure"), is(setOf(twain1, twain2)));
    }


    @Test
    public void addFirstRegistrationOfNewTopic() throws Exception {
        register(asimov1, bradbury1, twain1, tolkien1);

        assertThat(db.topics(), is(setOf("writer:scifi:books",
                                         "writer:adventure",
                                         "writer:adventure:tales")));

        assertThat(db.get("writer:scifi:books"), is(setOf(asimov1, bradbury1)));
        assertThat(db.get("writer:adventure"), is(setOf(twain1)));
        assertThat(db.get("writer:adventure:tales"), is(setOf(tolkien1)));
    }


    @Test
    public void addNextRegistrationOfNewTopic() throws Exception {
        register(asimov1, twain1, twain2);

        assertThat(db.get("writer:scifi:books"), is(setOf(asimov1)));
        assertThat(db.get("writer:adventure"), is(setOf(twain1, twain2)));
    }


    @Test
    public void addDuplicatedRegistrationDoesNothing() throws Exception {
        register(asimov1, bradbury1, bradbury1);

        assertThat(db.get("writer:scifi:books"), is(setOf(asimov1, bradbury1)));
    }


    @Test
    public void removeRegistrationFromOnlyTopicWithOneElement() throws Exception {
        register(asimov1);

        remove(asimov1);

        assertThat(db.find("writer", "scifi", "books"), is(empty()));
    }


    @Test
    public void removeRegistrationFromOnlyTopicWithSeveralElements() throws Exception {
        register(asimov1, asimov2, bradbury1);

        remove(asimov2);

        assertThat(db.find("writer", "scifi", "books"), is(setOf(asimov1, bradbury1)));
    }


    @Test
    public void removeRegistrationFromTopicWithOneElement() throws Exception {
        register(asimov1, twain1, twain2);

        remove(asimov1);

        assertThat(db.topics(), is(setOf("writer:adventure")));
        assertThat(db.get("writer:adventure"), is(setOf(twain1, twain2)));
    }


    @Test
    public void removeRegistrationFromTopicWithSeveralElements() throws Exception {
        register(asimov1, asimov2, bradbury1, twain1, twain2);

        remove(bradbury1);

        assertThat(db.get("writer:scifi:books"), is(setOf(asimov1, asimov2)));
        assertThat(db.get("writer:adventure"), is(setOf(twain1, twain2)));
    }


    @Test
    public void removeMissingRegistrationDoesNothing() throws Exception {
        register(asimov1, asimov2);

        remove(bradbury1);

        assertThat(db.get("writer:scifi:books"), is(setOf(asimov1, asimov2)));
    }


    @Test
    public void removeRegistrationByHost() throws Exception {
        registerAll();

        db.remove("10.2.9.2");

        assertThat(db.topics(), is(setOf("writer:scifi:books",
                                         "writer:adventure",
                                         "writer:adventure:tales")));

        assertThat(db.get("writer:scifi:books"), is(setOf(asimov1, bradbury1)));
        assertThat(db.get("writer:adventure"), is(setOf(twain1)));
        assertThat(db.get("writer:adventure:tales"), is(setOf(tolkien1)));
    }


    @Test
    public void removeLastRegistrationByData() throws Exception {
        register(asimov1);

        remove(asimov1);

        assertThat(db.topics(), is(empty()));
    }


    @Test
    public void removeLastRegistrationByHost() throws Exception {
        register(asimov1);

        db.remove("10.2.9.1");

        assertThat(db.topics(), is(empty()));
    }


    @Test
    public void findByDomain() throws Exception {
        register(asimov1, twain2, brando2, tolkien1);

        assertThat(db.find("writer", "*", "*"), is(setOf(asimov1, twain2, tolkien1)));
        assertThat(db.find("actor", "*", "*"), is(setOf(brando2)));
    }


    @Test
    public void findByDomainAndSubject() throws Exception {
        register(asimov1, bradbury2, twain1, twain2, brando2, tolkien1);

        assertThat(db.find("writer", "adventure", "*"), is(setOf(twain1, twain2, tolkien1)));
        assertThat(db.find("actor", "drama", "*"), is(empty()));
    }


    @Test
    public void findByFullTopic() throws Exception {
        register(asimov1, bradbury2, brando2, twain1, tolkien1);

        assertThat(db.find("writer", "scifi", "books"), is(setOf(asimov1, bradbury2)));
        assertThat(db.find("actor", "drama", "movies"), is(empty()));
    }


    @Test
    public void findUnregisteredTopicReturnsEmpty() throws Exception {
        register(asimov1, bradbury2, brando2, tolkien1);

        assertThat(db.find("writer", "adventure", "books"), is(empty()));
    }


    @Test
    public void reverseFindByDomain() throws Exception {
        register(asimov1, twain2, brando2, tolkien1);

        assertThat(db.rfind("writer", "*", "*"), is(empty()));
        assertThat(db.rfind("actor", "*", "*"), is(setOf(brando2)));
    }


    @Test
    public void reverseFindByDomainAndSubject() throws Exception {
        register(asimov1, bradbury2, twain1, twain2, brando2, tolkien1);

        assertThat(db.rfind("writer", "adventure", "*"), is(setOf(twain1, twain2)));
        assertThat(db.rfind("actor", "drama", "*"), is(setOf(brando2)));
    }


    @Test
    public void reverseFindByFullTopic() throws Exception {
        register(asimov1, bradbury2, brando2, twain1, tolkien1);

        assertThat(db.rfind("writer", "adventure", "tales"), is(setOf(twain1, tolkien1)));
        assertThat(db.rfind("actor", "drama", "movies"), is(setOf(brando2)));
    }


    @Test
    public void reverseFindUnregisteredTopicReturnsEmpty() throws Exception {
        register(asimov1, twain2, tolkien1);

        assertThat(db.rfind("writer", "scifi", "tales"), is(empty()));
    }


    @Test
    public void filterByDomain() throws Exception {
        registerAll();

        xMsgRegistration filter = newFilter().setDomain("writer").build();

        assertThat(db.filter(filter),
                   is(setOf(asimov1, asimov2, bradbury1, bradbury2, twain1, twain2, tolkien1)));
    }


    @Test
    public void filterBySubject() throws Exception {
        registerAll();

        xMsgRegistration filter = newFilter().setSubject("adventure").build();

        assertThat(db.filter(filter), is(setOf(twain1, twain2, tolkien1)));
    }


    @Test
    public void filterByType() throws Exception {
        registerAll();

        xMsgRegistration filter = newFilter().setType("books").build();

        assertThat(db.filter(filter),
                   is(setOf(asimov1, asimov2, bradbury1, bradbury2)));
    }


    @Test
    public void filterByAddress() throws Exception {
        registerAll();

        xMsgRegistration filter = newFilter().setHost("10.2.9.2").build();

        assertThat(db.filter(filter),
                   is(setOf(asimov2, bradbury2, brando2, twain2)));
    }


    @Test
    public void filterUnregisteredTopicReturnsEmpty() throws Exception {
        register(asimov1, twain2, tolkien1);

        xMsgRegistration filter = newFilter().setDomain("artist").build();

        assertThat(db.filter(filter), is(empty()));
    }


    @Test
    public void getSame() throws Exception {
        register(asimov1, brando2, twain1, twain2, tolkien1);

        assertThat(db.same("writer", "adventure", "*"), is(setOf(twain1, twain2)));
    }


    @Test
    public void getAll() throws Exception {
        registerAll();

        assertThat(db.all(), is(setOf(asimov1, asimov2, bradbury1, bradbury2,
                                      twain1, twain2, brando2, tolkien1)));
    }


    private static xMsgRegistration.Builder newRegistration(String name,
                                                            String host,
                                                            String topic) {
        return xMsgRegFactory.newRegistration(name, host, TYPE, xMsgTopic.wrap(topic));
    }


    private static xMsgRegistration.Builder newFilter() {
        return xMsgRegFactory.newFilter(TYPE);
    }


    private static Set<xMsgTopic> setOf(String... topics) {
        return Stream.of(topics).map(xMsgTopic::wrap).collect(Collectors.toSet());
    }


    private static Set<xMsgRegistration> setOf(xMsgRegistration.Builder... regs) {
        return Stream.of(regs).map(xMsgRegistration.Builder::build).collect(Collectors.toSet());
    }
}
