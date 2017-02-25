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

import org.jlab.coda.xmsg.core.xMsgTopic;
import org.jlab.coda.xmsg.core.xMsgUtil;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.Builder;
import org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration.OwnerType;

import java.util.Random;

public final class RegistrationDataFactory {

    public static String[] testTopics = {
        "writer",
        "writer:adventures",
        "writer:adventures:books",
        "writer:adventures:tales",
        "writer:scifi:books",
        "writer:scifi:tales",
        "actor",
        "actor:action",
        "actor:drama",
        "actor:comedy",
        "actor:action:movies",
        "actor:action:series",
        "actor:comedy:movies",
        "actor:comedy:series",
        "actor:drama:movies",
        "actor:drama:series",
    };
    public static String[] testNames = {"A", "B", "C", "D", "E",
                                        "F", "G", "H", "I", "J",
                                        "K", "L", "M", "N", "O",
                                        "P", "Q", "R", "S", "T",
                                        "U", "V", "W", "X", "Y",
                                        "Z"};
    public static String[] testHosts = {
        "10.2.9.50",  "10.2.9.60",
        "10.2.9.51",  "10.2.9.61",
        "10.2.9.52",  "10.2.9.62",
        "10.2.9.53",  "10.2.9.63",
        "10.2.9.54",  "10.2.9.64",
        "10.2.9.55",  "10.2.9.65",
        "10.2.9.56",  "10.2.9.66",
        "10.2.9.57",  "10.2.9.67",
        "10.2.9.58",  "10.2.9.68",
        "10.2.9.59",  "10.2.9.69",
    };
    private static Random rnd = new Random();

    private RegistrationDataFactory() {
    }


    public static Builder newRegistration(String name, OwnerType type, String topic) {
        return newRegistration(name, xMsgUtil.localhost(), type, topic);
    }


    public static Builder newRegistration(String name,
                                          String host,
                                          OwnerType type,
                                          String topic) {
        return xMsgRegFactory.newRegistration(name, host, type, xMsgTopic.wrap(topic));
    }


    public static String random(String[] array) {
        int idx = rnd.nextInt(array.length);
        return array[idx];
    }


    public static Builder randomRegistration() {
        String name = random(testNames);
        String host = random(testHosts);
        String topic = random(testTopics);
        OwnerType type = rnd.nextBoolean() ? OwnerType.PUBLISHER : OwnerType.SUBSCRIBER;
        return newRegistration(name, host, type, topic);
    }
}
