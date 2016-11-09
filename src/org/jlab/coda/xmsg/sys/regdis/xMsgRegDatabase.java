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

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 *    A registration database of xMsg actors.
 *    Actors are grouped by topic, i.e., actors registered with the same topic will
 *    be in the same group.
 *
 * @author smancill
 * @version 2.x
 */
class xMsgRegDatabase {

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
     * Returns a set with all actors whose topic is matched by the given topic.
     * Empty if no actor is found.
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
     * @return the set of all actors that are matched by the topic
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
     * Returns a set with all actors whose topic matches the given topic.
     * Empty if no actor is found.
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
     * find("DOMAIN", "*", "*")           -->  3
     * find("DOMAIN", "SUBJECT", "*")     -->  3, 2
     * find("DOMAIN", "SUBJECT", "TYPE")  -->  3, 2, 1
     * </pre>
     *
     * @param domain the searched domain
     * @param subject the searched type (it can be undefined)
     * @param type the searched type (it can be undefined)
     * @return the set of all actors that match the topic
     */
    public Set<xMsgRegistration> rfind(String domain, String subject, String type) {
        Set<xMsgRegistration> result = new HashSet<>();
        xMsgTopic searchedTopic = xMsgTopic.build(domain, subject, type);
        for (xMsgTopic topic : db.keySet()) {
            if (topic.isParent(searchedTopic)) {
                result.addAll(db.get(topic));
            }
        }
        return result;
    }

    /**
     * Returns a set with all actors whose registration exactly matches the
     * given terms. Empty if no actor is found.
     * <p>
     * The search terms can be:
     * <ul>
     * <li>domain
     * <li>subject
     * <li>type
     * <li>address
     * </ul>
     * Only defined terms will be used for matching actors.
     * The topic parts are undefined if its value is {@link xMsgConstants#ANY}.
     * The address is undefined if its value is {@link xMsgConstants#UNDEFINED}.
     *
     * @param data the searched terms
     * @return the set of all actors that match the terms
     */
    public Set<xMsgRegistration> filter(xMsgRegistration data) {
        Filter filter = new Filter(data);
        for (Entry<xMsgTopic, Set<xMsgRegistration>> entry : db.entrySet()) {
            filter.filter(entry.getKey(), entry.getValue());
        }
        return filter.result();
    }


    /**
     * Returns all registered actors.
     *
     * @return the set of all actors
     */
    public Set<xMsgRegistration> all() {
        return db.values()
                 .stream()
                 .flatMap(Set::stream)
                 .collect(Collectors.toSet());
    }


    /**
     * Returns all registered topics.
     *
     * @see #get
     */
    public Set<xMsgTopic> topics() {
        return db.keySet();
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



    private final class Filter {

        private final Set<xMsgRegistration> result = new HashSet<>();

        private final TopicFilter domain;
        private final TopicFilter subject;
        private final TopicFilter type;
        private final AddressFilter address;

        /**
         * Cache a topic term.
         * Avoid checking if the value is any for every actor.
         */
        private final class TopicFilter {
            private final boolean any;
            private final String value;

            private TopicFilter(String value) {
                this.any = value.equals(xMsgConstants.ANY);
                this.value = value;
            }
        }

        /**
         * Cache the address
         * Avoid checking if the address is set for every actor.
         */
        private final class AddressFilter {
            private final boolean filter;
            private final String host;
            private final int port;

            private AddressFilter(xMsgRegistration data) {
                this.host = data.getHost();
                this.port = data.getPort();
                this.filter = !host.equals(xMsgConstants.UNDEFINED);
            }
        }

        private Filter(xMsgRegistration data) {
            this.domain = new TopicFilter(data.getDomain());
            this.subject = new TopicFilter(data.getSubject());
            this.type = new TopicFilter(data.getType());
            this.address = new AddressFilter(data);
        }

        public void filter(xMsgTopic topic, Set<xMsgRegistration> actors) {
            if (matchTopic(topic)) {
                if (filterAddress()) {
                    actors.stream().filter(this::matchAddress).forEach(result::add);
                } else {
                    result.addAll(actors);
                }
            }
        }

        public Set<xMsgRegistration> result() {
            return result;
        }

        private boolean matchTopic(xMsgTopic topic) {
            return (domain.any  || topic.domain().equals(domain.value))
                && (subject.any || topic.subject().equals(subject.value))
                && (type.any    || topic.type().equals(type.value));
        }

        private boolean filterAddress() {
            return address.filter;
        }

        private boolean matchAddress(xMsgRegistration actor) {
            return actor.getHost().equals(address.host) && actor.getPort() == address.port;
        }
    }
}
