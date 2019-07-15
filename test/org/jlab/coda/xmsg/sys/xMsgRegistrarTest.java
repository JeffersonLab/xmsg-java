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

package org.jlab.coda.xmsg.sys;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgRegQuery;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgContext;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.sys.regdis.RegistrationDataFactory;
import org.jlab.coda.xmsg.sys.regdis.xMsgRegDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

@Tag("integration")
public class xMsgRegistrarTest {

    private xMsgRegDriver driver;

    private Set<xMsgRegistration> registration = new HashSet<>();
    private String name = "registrat_test";

    @Test
    public void testRegistrationDataBase() throws Exception {
        xMsgContext context = xMsgContext.newContext();
        xMsgRegistrar registrar = null;
        try {
            try {
                registrar = new xMsgRegistrar(context);
                registrar.start();
            } catch (xMsgException e) {
                System.err.println(e.getMessage());
            }

            xMsgConnectionFactory factory = new xMsgConnectionFactory(context);
            driver = factory.createRegistrarConnection(new xMsgRegAddress());
            xMsgUtil.sleep(200);

            long start = System.currentTimeMillis();

            addRandom(10000);
            check();

            removeRandom(2500);
            check();

            addRandom(1000);
            check();

            removeRandomHost();
            check();

            addRandom(1000);
            check();

            removeRandom(2500);
            filter();

            addRandom(1000);
            same();

            removeRandom(2500);
            all();

            removeAll();
            check();

            long end = System.currentTimeMillis();
            System.out.println("Total time: " + (end - start) / 1000.0);
        } finally {
            if (driver != null) {
                driver.close();
            }
            context.close();
            if (registrar != null) {
                registrar.shutdown();
            }
        }
    }


    public void addRandom(int size) throws xMsgException {
        System.out.println("INFO: Registering " + size + " random actors...");
        for (int i = 0; i < size; i++) {
            Builder rndReg = RegistrationDataFactory.randomRegistration();
            xMsgRegistration data = rndReg.build();
            driver.addRegistration(name, data);
            registration.add(data);
        }
    }


    public void removeRandom(int size) throws xMsgException {
        System.out.println("INFO: Removing " + size + " random actors...");

        int first = new Random().nextInt(registration.size() - size);
        int end = first + size;
        int i = 0;
        Iterator<xMsgRegistration> it = registration.iterator();
        while (it.hasNext()) {
            if (i == end) {
                break;
            }
            xMsgRegistration reg = it.next();
            if (i >= first) {
                it.remove();
                driver.removeRegistration(name, reg);
            }
            i++;
        }
    }


    public void removeRandomHost() throws xMsgException {
        String host = RegistrationDataFactory.random(RegistrationDataFactory.testHosts);
        removeHost(host);
    }


    private void removeHost(String host) throws xMsgException {
        System.out.println("INFO: Removing host " + host);
        registration.removeIf(r -> r.getHost().equals(host));
        driver.removeAllRegistration("test", host);
    }


    public void removeAll() throws xMsgException {
        for (String host : RegistrationDataFactory.testHosts) {
            driver.removeAllRegistration("test", host);
        }
        registration.clear();
    }


    public void check() throws xMsgException {
        checkActors(OwnerType.PUBLISHER);
        checkActors(OwnerType.SUBSCRIBER);
    }


    public void filter() throws xMsgException {
        filterActors(OwnerType.PUBLISHER);
        filterActors(OwnerType.SUBSCRIBER);
    }


    public void same() throws xMsgException {
        compareActors(OwnerType.PUBLISHER);
        compareActors(OwnerType.SUBSCRIBER);
    }


    public void all() throws xMsgException {
        allActors(OwnerType.PUBLISHER);
        allActors(OwnerType.SUBSCRIBER);
    }


    private void checkActors(OwnerType regType) throws xMsgException {
        ResultAssert checker = new ResultAssert("topic", regType);
        for (String topic : RegistrationDataFactory.testTopics) {
            Builder data = getQuery(regType).matching(xMsgTopic.wrap(topic)).data();
            Predicate<xMsgRegistration> predicate = discoveryPredicate(regType, topic);

            Set<xMsgRegistration> result = driver.findRegistration(name, data.build());
            Set<xMsgRegistration> expected = find(regType, predicate);

            checker.assertThat(topic, result, expected);
        }
    }

    private Predicate<xMsgRegistration> discoveryPredicate(OwnerType regType, String topic) {
        final xMsgTopic searchTopic = xMsgTopic.wrap(topic);
        if (regType == OwnerType.PUBLISHER) {
            return r -> searchTopic.isParent(getTopic(r));
        } else {
            return r -> getTopic(r).isParent(searchTopic);
        }
    }


    private void filterActors(OwnerType regType) throws xMsgException {
        filterByDomain(regType);
        filterBySubject(regType);
        filterByType(regType);
        filterByHost(regType);
    }


    private void filterByDomain(OwnerType regType) throws xMsgException {
        Set<String> domains = Stream.of(RegistrationDataFactory.testTopics)
                                    .map(xMsgTopic::wrap)
                                    .map(xMsgTopic::domain)
                                    .filter(t -> !t.equals(xMsgConstants.ANY))
                                    .collect(Collectors.toSet());

        ResultAssert checker = new ResultAssert("domain", regType);
        for (String domain : domains) {
            Builder data = getQuery(regType).withDomain(domain).data();

            Set<xMsgRegistration> result = driver.filterRegistration(name, data.build());
            Set<xMsgRegistration> expected = find(regType, e -> e.getDomain().equals(domain));

            checker.assertThat(domain, result, expected);
        }
    }


    private void filterBySubject(OwnerType regType) throws xMsgException {
        Set<String> subjects = Stream.of(RegistrationDataFactory.testTopics)
                                      .map(xMsgTopic::wrap)
                                      .map(xMsgTopic::subject)
                                      .filter(t -> !t.equals(xMsgConstants.ANY))
                                      .collect(Collectors.toSet());

        ResultAssert checker = new ResultAssert("subject", regType);
        for (String subject : subjects) {
            Builder data = getQuery(regType).withSubject(subject).data();

            Set<xMsgRegistration> result = driver.filterRegistration(name, data.build());
            Set<xMsgRegistration> expected = find(regType, e -> e.getSubject().equals(subject));

            checker.assertThat(subject, result, expected);
        }
    }


    private void filterByType(OwnerType regType) throws xMsgException {
        Set<String> types = Stream.of(RegistrationDataFactory.testTopics)
                                  .map(xMsgTopic::wrap)
                                  .map(xMsgTopic::type)
                                  .filter(t -> !t.equals(xMsgConstants.ANY))
                                  .collect(Collectors.toSet());

        ResultAssert checker = new ResultAssert("type", regType);
        for (String type : types) {
            Builder data = getQuery(regType).withType(type).data();

            Set<xMsgRegistration> result = driver.filterRegistration(name, data.build());
            Set<xMsgRegistration> expected = find(regType, e -> e.getType().equals(type));

            checker.assertThat(type, result, expected);
        }
    }


    private void filterByHost(OwnerType regType) throws xMsgException {
        ResultAssert checker = new ResultAssert("host", regType);
        for (String host : RegistrationDataFactory.testHosts) {
            Builder data = getQuery(regType).withHost(host).data();

            Set<xMsgRegistration> result = driver.filterRegistration(name, data.build());
            Set<xMsgRegistration> expected = find(regType, e -> e.getHost().equals(host));

            checker.assertThat(host, result, expected);
        }
    }


    private void compareActors(OwnerType regType) throws xMsgException {
        ResultAssert checker = new ResultAssert("topic", regType);
        for (String topic : RegistrationDataFactory.testTopics) {
            xMsgTopic searchTopic = xMsgTopic.wrap(topic);
            Builder data = getQuery(regType).withSame(searchTopic).data();

            Set<xMsgRegistration> result = driver.sameRegistration(name, data.build());
            Set<xMsgRegistration> expected = find(regType, r -> getTopic(r).equals(searchTopic));

            checker.assertThat(topic, result, expected);
        }
    }


    private void allActors(OwnerType regType) throws xMsgException {
        Builder data = getQuery(regType).all().data();

        Set<xMsgRegistration> result = driver.filterRegistration(name, data.build());
        Set<xMsgRegistration> expected = find(regType, e -> true);

        String owner = regType == OwnerType.PUBLISHER ? "publishers" : "subscribers";
        if (result.equals(expected)) {
            System.out.printf("Found %3d %s%n", result.size(), owner);
        } else {
            System.out.println("All: " + owner);
            System.out.println("Result: " + result.size());
            System.out.println("Expected: " + expected.size());
            fail("Sets doesn't match!!!");
        }
    }


    private Set<xMsgRegistration> find(OwnerType regType,
                                       Predicate<xMsgRegistration> predicate) {
        return registration.stream()
                           .filter(r -> r.getOwnerType() == regType)
                           .filter(predicate)
                           .collect(Collectors.toSet());
    }


    private xMsgTopic getTopic(xMsgRegistration reg) {
        return xMsgTopic.build(reg.getDomain(), reg.getSubject(), reg.getType());
    }


    private xMsgRegQuery.Factory getQuery(OwnerType regType) {
        if (regType == OwnerType.PUBLISHER) {
            return xMsgRegQuery.publishers();
        } else {
            return xMsgRegQuery.subscribers();
        }
    }


    private static final class ResultAssert {

        private final String valueName;
        private final OwnerType regType;

        private ResultAssert(String valueName, OwnerType regType) {
            this.valueName = valueName;
            this.regType = regType;
        }

        private void assertThat(String data,
                                Set<xMsgRegistration> result,
                                Set<xMsgRegistration> expected) {
            if (result.equals(expected)) {
                String owner = regType == OwnerType.PUBLISHER ? "publishers" : "subscribers";
                System.out.printf("Found %3d %s with %s %s%n",
                                  result.size(), owner, valueName, data);
            } else {
                String outName = valueName.substring(0, 1).toUpperCase() + valueName.substring(1);
                System.out.println(outName + ": " + data);
                System.out.println("Result: " + result.size());
                System.out.println("Expected: " + expected.size());
                fail("Sets doesn't match!!!");
            }
        }
    }
}
