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

package org.jlab.coda.xmsg.xsys;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.testing.IntegrationTest;
import org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class xMsgRegistrarTest {

    private xMsgRegistrar registrar;
    private xMsgRegDriver driver;

    private Set<xMsgRegistration> registration = new HashSet<>();
    private String name = "registrat_test";

    @Test
    public void testRegistrationDataBase() throws Exception {
        try {
            driver = new xMsgRegDriver("localhost");
            registrar = new xMsgRegistrar();
            registrar.start();
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

            removeAll();
            check();

            long end = System.currentTimeMillis();
            System.out.println("Total time: " + (end - start) / 1000.0);
        } finally {
            if (registrar != null) {
                registrar.shutdown();
            }
        }
    }


    public void addRandom(int size) throws xMsgRegistrationException {
        System.out.println("INFO: Registering " + size + " random actors...");
        for (int i = 0; i < size; i++) {
            Builder rndReg = RegistrationDataFactory.randomRegistration();
            xMsgRegistration data = rndReg.build();
            driver.registerLocal(data.getName(), data, checkPublisher(data));
            registration.add(data);
        }
    }


    public void removeRandom(int size) throws xMsgRegistrationException {
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
                driver.removeRegistrationLocal(name, reg, checkPublisher(reg));
            }
            i++;
        }
    }


    public void removeRandomHost() throws xMsgRegistrationException {
        String host = RegistrationDataFactory.random(RegistrationDataFactory.testHosts);
        removeHost(host);
    }


    private void removeHost(String host) throws xMsgRegistrationException {
        System.out.println("INFO: Removing host " + host);
        Iterator<xMsgRegistration> it = registration.iterator();
        while (it.hasNext()) {
            xMsgRegistration reg = it.next();
            if (reg.getHost().equals(host)) {
                it.remove();
            }
        }
        driver.removeAllRegistrationFE(host, name);
    }


    public void removeAll() throws xMsgRegistrationException {
        for (String host : RegistrationDataFactory.testHosts) {
            driver.removeAllRegistrationFE(host, name);
        }
        registration.clear();
    }


    public void check() throws xMsgRegistrationException {
        check(true);
        check(false);
    }


    private void check(boolean isPublisher) throws xMsgRegistrationException {
        for (String topic : RegistrationDataFactory.testTopics) {
            Builder data = discoveryRequest(topic, isPublisher);

            Set<xMsgRegistration> result = driver.findLocal(name, data.build(), isPublisher);
            Set<xMsgRegistration> expected = find(topic, isPublisher);

            if (result.equals(expected)) {
                String owner = isPublisher ? "publishers" : "subscribers";
                System.out.printf("Found %3d %s for %s%n", result.size(), owner, topic);
            } else {
                System.out.println("Topic: " + topic);
                System.out.println("Result: " + result.size());
                System.out.println("Expected: " + expected.size());
                fail("Sets doesn't match!!!");
            }
        }
    }


    private Builder discoveryRequest(String topic, boolean isPublisher) {
        return RegistrationDataFactory.newRegistration(name, "localhost", topic, isPublisher);
    }


    private Set<xMsgRegistration> find(String topic, boolean isPublisher) {
        Set<xMsgRegistration> set = new HashSet<>();
        xMsgTopic searchTopic = xMsgTopic.wrap(topic);
        for (xMsgRegistration reg : registration) {
            if (isPublisher != checkPublisher(reg)) {
                continue;
            }
            xMsgTopic regTopic = xMsgTopic.build(reg.getDomain(), reg.getSubject(), reg.getType());
            if (searchTopic.isParent(regTopic)) {
                set.add(reg);
            }
        }
        return set;
    }


    private boolean checkPublisher(xMsgRegistration reg) {
        return reg.getOwnerType() == xMsgRegistration.OwnerType.PUBLISHER;
    }
}