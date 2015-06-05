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

import java.net.SocketException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgRegistrationException;
import org.jlab.coda.xmsg.xsys.regdis.RegistrationDataFactory;


public final class xMsgRegistrarTest implements AutoCloseable {

    public static void main(String[] args) {
        try (xMsgRegistrarTest test = new xMsgRegistrarTest()) {
            long start = System.currentTimeMillis();

            test.addRandom(10000);
            test.check();

            test.removeRandom(2500);
            test.check();

            test.addRandom(1000);
            test.check();

            test.removeRandomHost();
            test.check();

            test.addRandom(1000);
            test.check();

            test.removeAll();
            test.check();

            long end = System.currentTimeMillis();
            System.out.println("Total time: " + (end - start) / 1000.0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private xMsgRegistrar registrar;
    private Set<xMsgRegistration> registration = new HashSet<>();
    private String name = "registrat_test";


    private xMsgRegistrarTest() throws SocketException, xMsgException {
        registrar = new xMsgRegistrar();
        registrar.start();
    }


    @Override
    public void close() throws Exception {
        registrar.shutdown();
    }


    public void addRandom(int size) throws xMsgRegistrationException {
        System.out.println("INFO: Registering " + size + " random actors...");
        for (int i = 0; i < size; i++) {
            Builder rndReg = RegistrationDataFactory.randomRegistration();
            xMsgRegistration data = rndReg.build();
            registrar.registerLocal(data.getName(), data, checkPublisher(data));
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
                registrar.removeRegistrationLocal(name, reg, checkPublisher(reg));
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
        registrar.removeAllRegistrationFE(host, name);
    }


    public void removeAll() throws xMsgRegistrationException {
        for (String host : RegistrationDataFactory.testHosts) {
            registrar.removeAllRegistrationFE(host, name);
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

            Set<xMsgRegistration> result = registrar.findLocal(name, data.build(), isPublisher);
            Set<xMsgRegistration> expected = find(topic, isPublisher);

            if (result.equals(expected)) {
                String owner = isPublisher ? "publishers" : "subscribers";
                System.out.printf("Found %3d %s for %s%n", result.size(), owner, topic);
            } else {
                System.out.println("Topic: " + topic);
                System.out.println("Result: " + result.size());
                System.out.println("Expected: " + expected.size());
                throw new RuntimeException("Sets doesn't match!!!");
            }
        }
    }


    private Builder discoveryRequest(String topic, boolean isPublisher) {
        return RegistrationDataFactory.newRegistration(name, "localhost", topic, isPublisher);
    }


    private Set<xMsgRegistration> find(String topic, boolean isPublisher) {
        Set<xMsgRegistration> set = new HashSet<>();
        for (xMsgRegistration reg : registration) {
            if (isPublisher != checkPublisher(reg)) {
                continue;
            }
            String regTopic = xMsgUtil.buildTopic(reg.getDomain(), reg.getSubject(), reg.getType());
            if (regTopic.startsWith(topic)) {
                set.add(reg);
            }
        }
        return set;
    }


    private boolean checkPublisher(xMsgRegistration reg) {
        return reg.getOwnerType() == xMsgRegistration.OwnerType.PUBLISHER;
    }
}
