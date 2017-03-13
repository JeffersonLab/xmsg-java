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
 * @version 2.x
 */
class xMsgRegDatabase {

    private final ConcurrentMap<String, ConcurrentMap<xMsgTopic, Set<xMsgRegistration>>>
            db = new ConcurrentHashMap<>();

    /**
     * Adds a new xMsg actor to the registration.
     * The actor will be grouped along with all other actors with the same
     * topic. If the actor is already registered, nothing is changed.
     *
     * @param regData the description of the actor
     */
    public void register(xMsgRegistration regData) {
        ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> map =
                db.computeIfAbsent(regData.getDomain(), k -> new ConcurrentHashMap<>());
        xMsgTopic key = generateKey(regData);
        if (map.containsKey(key)) {
            map.get(key).add(regData);
        } else {
            Set<xMsgRegistration> regSet = new HashSet<>();
            regSet.add(regData);
            map.put(key, regSet);
        }
    }


    /**
     * Removes the given actor from the registration.
     *
     * @param regData the description of the actor
     */
    public void remove(xMsgRegistration regData) {
        ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> map = db.get(regData.getDomain());
        if (map == null) {
            return;
        }
        xMsgTopic key = generateKey(regData);
        if (map.containsKey(key)) {
            Set<xMsgRegistration> set = map.get(key);
            set.removeIf(r -> r.getName().equals(regData.getName())
                           && r.getHost().equals(regData.getHost()));
            if (set.isEmpty()) {
                map.remove(key);
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
        for (ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> map : db.values()) {
            Iterator<Entry<xMsgTopic, Set<xMsgRegistration>>> dbIt = map.entrySet().iterator();
            while (dbIt.hasNext()) {
                Entry<xMsgTopic, Set<xMsgRegistration>> dbEntry = dbIt.next();
                dbEntry.getValue().removeIf(reg -> reg.getHost().equals(host));
                if (dbEntry.getValue().isEmpty()) {
                    dbIt.remove();
                }
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
        ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> map = db.get(domain);
        if (map == null) {
            return result;
        }
        xMsgTopic searchedTopic = xMsgTopic.build(domain, subject, type);
        for (xMsgTopic topic : map.keySet()) {
            if (searchedTopic.isParent(topic)) {
                result.addAll(map.get(topic));
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
        ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> map = db.get(domain);
        if (map == null) {
            return result;
        }
        xMsgTopic searchedTopic = xMsgTopic.build(domain, subject, type);
        for (xMsgTopic topic : map.keySet()) {
            if (topic.isParent(searchedTopic)) {
                result.addAll(map.get(topic));
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
     * The address is undefined if its value is {@link xMsgRegConstants#UNDEFINED}.
     *
     * @param data the searched terms
     * @return the set of all actors that match the terms
     */
    public Set<xMsgRegistration> filter(xMsgRegistration data) {
        Filter filter = new Filter(data);
        for (Entry<String, ConcurrentMap<xMsgTopic, Set<xMsgRegistration>>> level : db.entrySet()) {
            if (!filter.matchDomain(level.getKey())) {
                continue;
            }
            for (Entry<xMsgTopic, Set<xMsgRegistration>> entry : level.getValue().entrySet()) {
                filter.filter(entry.getKey(), entry.getValue());
            }
        }
        return filter.result();
    }


    /**
     * Returns a set with all actors whose topic is the same as the given topic.
     * Empty if no actor is found.
     * <p>
     * The topics must be equals. No prefix matching is done.
     * If we have actors registered to these topics:
     * <ol>
     * <li>{@code "DOMAIN:SUBJECT:TYPE"}
     * <li>{@code "DOMAIN:SUBJECT"}
     * <li>{@code "DOMAIN"}
     * </ol>
     * then this will be returned:
     * <pre>
     * find("DOMAIN", "*", "*")           -->  3
     * find("DOMAIN", "SUBJECT", "*")     -->  2
     * find("DOMAIN", "SUBJECT", "TYPE")  -->  1
     * </pre>
     *
     * @param domain the searched domain
     * @param subject the searched type (it can be undefined)
     * @param type the searched type (it can be undefined)
     * @return the set of all actors that have the same topic
     */
    public Set<xMsgRegistration> same(String domain, String subject, String type) {
        Set<xMsgRegistration> result = new HashSet<>();
        ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> map = db.get(domain);
        if (map == null) {
            return result;
        }
        xMsgTopic searchedTopic = xMsgTopic.build(domain, subject, type);
        for (xMsgTopic topic : map.keySet()) {
            if (searchedTopic.equals(topic)) {
                result.addAll(map.get(topic));
            }
        }
        return result;
    }


    /**
     * Returns all registered actors.
     *
     * @return the set of all actors
     */
    public Set<xMsgRegistration> all() {
        return db.values()
                 .stream()
                 .flatMap(m -> m.values().stream())
                 .flatMap(s -> s.stream())
                 .collect(Collectors.toSet());
    }


    /**
     * Returns all registered topics.
     *
     * @see #get
     */
    public Set<xMsgTopic> topics() {
        return db.values()
                 .stream()
                 .flatMap(m -> m.keySet().stream())
                 .collect(Collectors.toSet());
    }


    /**
     * Returns all actors registered with the specific known topic.
     *
     * @see #topics
     */
    public Set<xMsgRegistration> get(String topic) {
        return get(xMsgTopic.wrap(topic));
    }


    /**
     * Returns all actors registered with the specific known topic.
     *
     * @see #topics
     */
    public Set<xMsgRegistration> get(xMsgTopic topic) {
        ConcurrentMap<xMsgTopic, Set<xMsgRegistration>> map = db.get(topic.domain());
        if (map == null) {
            return new HashSet<>();
        }
        Set<xMsgRegistration> result = map.get(topic);
        if (result == null) {
            return new HashSet<>();
        }
        return result;
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
                this.filter = !host.equals(xMsgRegConstants.UNDEFINED);
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

        private boolean filterAddress() {
            return address.filter;
        }

        public Set<xMsgRegistration> result() {
            return result;
        }

        public boolean matchDomain(String regDomain) {
            return domain.any | regDomain.equals(domain.value);
        }

        public boolean matchTopic(xMsgTopic topic) {
            return (domain.any  || topic.domain().equals(domain.value))
                && (subject.any || topic.subject().equals(subject.value))
                && (type.any    || topic.type().equals(type.value));
        }

        public boolean matchAddress(xMsgRegistration actor) {
            return actor.getHost().equals(address.host)
                    && (address.port == 0 || actor.getPort() == address.port);
        }
    }
}
