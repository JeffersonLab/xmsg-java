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

package org.jlab.coda.xmsg.xsys.regdis;

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *    A registration database of xMsg actors.
 *    Actors are grouped by topic, i.e., actors registered with the same topic will
 *    be in the same group.
 *
 * @author smancill
 * @version 2.x
 */
public class xMsgRegDatabase {

    private final ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> db =  new ConcurrentHashMap<>();

    /**
     * Adds a new xMsg actor to the registration.
     * The actor will be grouped along with all other actors with the same
     * topic. If the actor is already registered, nothing is changed.
     *
     * @param regData the description of the actor
     */
    public void register(xMsgRegistration regData) {
        xMsgTopic key = generateKey(regData);
        if (db.containsKey(key)) {
            db.get(key).add(regData);
        } else {
            Set<xMsgRegistration> regSet = new HashSet<>();
            regSet.remove(null);
            regSet.add(regData);
            db.put(key, regSet);

        }
    }


    /**
     * Removes the given actor from the registration.
     *
     * @param regData the description of the actor
     */
    public void remove(xMsgRegistration regData) {
        xMsgTopic key = generateKey(regData);
        if (db.containsKey(key)) {
            Set<xMsgRegistration> set = db.get(key);
            for (Iterator<xMsgRegistration> i = set.iterator(); i.hasNext();) {
                xMsgRegistration r = i.next();
                if (r.getName().equals(regData.getName()) &&
                        r.getHost().equals(regData.getHost())) {
                    i.remove();
                }
            }
            if (set.isEmpty()) {
                db.remove(key);
            }
        }
    }


    /**
     * Removes all actors on the given host from the registration.
     * Useful when a xMsg node will be shutdown, so all actors running in the
     * node have to be unregistered.
     *
     * @param host the host of the actors that should be removed
     */
    public void remove(String host) {
        Iterator<Entry<xMsgTopic, Set<xMsgRegistration>>> dbIt = db.entrySet().iterator();
        while (dbIt.hasNext()) {
            Entry<xMsgTopic, Set<xMsgRegistration>> dbEntry = dbIt.next();
            Iterator<xMsgRegistration> regIt = dbEntry.getValue().iterator();
            while (regIt.hasNext()) {
                xMsgRegistration reg = regIt.next();
                if (reg.getHost().equals(host)) {
                    regIt.remove();
                }
            }
            if (dbEntry.getValue().isEmpty()) {
                dbIt.remove();
            }
        }
    }


    private xMsgTopic generateKey(xMsgRegistration regData) {
        return xMsgTopic.build(regData.getDomain(), regData.getSubject(), regData.getType());
    }


    /**
     * Returns a set with all actors registered to the given topic. Empty if no
     * actor is found.
     * <p>
     * The rules to match topics are the following.
     * If we have actors registered to these topics:
     * <ol>
     * <li>{@code "DOMAIN:SUBJECT:TYPE"}
     * <li>{@code "DOMAIN:SUBJECT"}
     * <li>{@code "DOMAIN"}
     * </ol>
     * then this will be returned:
     * <pre>
     * find("DOMAIN", "*", "*")           -->  1, 2, 3
     * find("DOMAIN", "SUBJECT", "*")     -->  1, 2
     * find("DOMAIN", "SUBJECT", "TYPE")  -->  1
     * </pre>
     *
     * @param domain the searched domain
     * @param subject the searched type (it can be undefined)
     * @param type the searched type (it can be undefined)
     * @return a set of all actors registered to the topic
     */
    public Set<xMsgRegistration> find(String domain, String subject, String type) {
        Set<xMsgRegistration> result = new HashSet<>();
        xMsgTopic searchedTopic = xMsgTopic.build(domain, subject, type);
        for (xMsgTopic topic : db.keySet()) {
            if (searchedTopic.isParent(topic)) {
                result.addAll(db.get(topic));
            }
        }
        return result;
    }


    /**
     * Returns all registered topics.
     *
     * @see #get
     */
    public Set<xMsgTopic> topics() {
        return db.keySet();
    }

    public String findDomainNames() {
        StringBuilder result = new StringBuilder();
        for (xMsgTopic topic : db.keySet()) {
            result.append(topic.domain()).append(" ");
        }
        return result.toString();
    }

    public String findSubjectNames(String domainName) {
        StringBuilder result = new StringBuilder();
        for (xMsgTopic topic : db.keySet()) {
            if (topic.domain().equals(domainName)) {
                result.append(topic.subject()).append(" ");
            }
        }
        return result.toString();
    }

    public String findTypeNames(String domainName, String subjectName) {
        StringBuilder result = new StringBuilder();
        for (xMsgTopic topic : db.keySet()) {
            if (topic.domain().equals(domainName) && topic.subject().equals(subjectName)) {
                result.append(topic.type()).append(" ");
            }
        }
        return result.toString();
    }

    /**
     * Returns all actors registered with the specific known topic.
     *
     * @see #topics
     */
    public Set<xMsgRegistration> get(String topic) {
        return db.get(xMsgTopic.wrap(topic));
    }


    /**
     * Returns all actors registered with the specific known topic.
     *
     * @see #topics
     */
    public Set<xMsgRegistration> get(xMsgTopic topic) {
        return db.get(topic);
    }
}
