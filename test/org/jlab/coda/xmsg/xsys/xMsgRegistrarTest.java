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

package org.jlab.coda.xmsg.xsys;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgConnectionFactory;
import org.jlab.coda.xmsg.net.xMsgRegAddress;
import org.jlab.coda.xmsg.testing.IntegrationTest;
import org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory;
import org.jlab.coda.xmsg.xsys.regdis.xMsgRegDriver;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.zeromq.ZContext;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class xMsgRegistrarTest {

    private xMsgRegDriver driver;

    private Set<xMsgRegistration> registration = new HashSet<>();
    private String name = "registrat_test";

    @Test
    public void testRegistrationDataBase() throws Exception {
        ZContext context = new ZContext();
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

            removeAll();
            check();

            long end = System.currentTimeMillis();
            System.out.println("Total time: " + (end - start) / 1000.0);
        } finally {
            driver.close();
            context.destroy();
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
        Iterator<xMsgRegistration> it = registration.iterator();
        while (it.hasNext()) {
            xMsgRegistration reg = it.next();
            if (reg.getHost().equals(host)) {
                it.remove();
            }
        }
        driver.removeAllRegistration("test", host);
    }


    public void removeAll() throws xMsgException {
        for (String host : RegistrationDataFactory.testHosts) {
            driver.removeAllRegistration("test", host);
        }
        registration.clear();
    }


    public void check() throws xMsgException {
        check(OwnerType.PUBLISHER);
        check(OwnerType.SUBSCRIBER);
    }


    private void check(OwnerType regType) throws xMsgException {
        for (String topic : RegistrationDataFactory.testTopics) {
            Builder data = discoveryRequest(regType, topic);

            Set<xMsgRegistration> result = driver.findRegistration(name, data.build());
            Set<xMsgRegistration> expected = find(regType, topic);

            if (result.equals(expected)) {
                String owner = regType == OwnerType.PUBLISHER ? "publishers" : "subscribers";
                System.out.printf("Found %3d %s for %s%n", result.size(), owner, topic);
            } else {
                System.out.println("Topic: " + topic);
                System.out.println("Result: " + result.size());
                System.out.println("Expected: " + expected.size());
                fail("Sets doesn't match!!!");
            }
        }
    }


    private Builder discoveryRequest(OwnerType regType, String topic) {
        return RegistrationDataFactory.newRegistration("", regType, topic);
    }


    private Set<xMsgRegistration> find(OwnerType regType, String topic) {
        Set<xMsgRegistration> set = new HashSet<>();
        xMsgTopic searchTopic = xMsgTopic.wrap(topic);
        for (xMsgRegistration reg : registration) {
            if (reg.getOwnerType() != regType) {
                continue;
            }
            xMsgTopic regTopic = xMsgTopic.build(reg.getDomain(), reg.getSubject(), reg.getType());
            if (regType == OwnerType.PUBLISHER) {
                if (searchTopic.isParent(regTopic)) {
                    set.add(reg);
                }
            } else {
                if (regTopic.isParent(searchTopic)) {
                    set.add(reg);
                }
            }
        }
        return set;
    }
}
