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

package org.jlab.coda.xmsg.core;

import com.google.protobuf.ByteString;

import org.jlab.coda.xmsg.excp.xMsgException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * xMsg utility methods.
 *
 * @author gurjyan
 * @since 1.0
 */

public final class xMsgUtil {

    private static List<String> localHostIps = new ArrayList<>();

    private xMsgUtil() { }


    /**
     * Returns formatted string of the current date and time.
     * Options 1 through 10 are accepted, producing following formatting:
     * <p>
     * <ol>
     * <li>Tue Nov 04 20:14:11 EST 2003</li>
     * <li>11/4/03 8:14 PM</li>
     * <li>8:14:11 PM</li>
     * <li>Nov 4, 2003 8:14:11 PM</li>
     * <li>8:14 PM</li>
     * <li>8:14:11 PM</li>
     * <li>8:14:11 PM EST</li>
     * <li>11/4/03 8:14 PM</li>
     * <li>Nov 4, 2003 8:14 PM</li>
     * <li>November 4, 2003 8:14:11 PM EST</li>
     * </ol>
     *
     * @param type the option of the formatting.
     * @return formatted string of the current data
     */
    public static String currentTime(int type) {

        // Make a new Date object.
        // It will be initialized to the current time.
        Date now = new Date();

        switch (type) {
            case 1:
                return now.toString();
            case 2:
                return DateFormat.getInstance().format(now);
            case 3:
                return DateFormat.getTimeInstance().format(now);
            case 4:
                return DateFormat.getDateTimeInstance().format(now);
            case 5:
                return DateFormat.getTimeInstance(DateFormat.SHORT).format(now);
            case 6:
                return DateFormat.getTimeInstance(DateFormat.MEDIUM).format(now);
            case 7:
                return DateFormat.getTimeInstance(DateFormat.LONG).format(now);
            case 8:
                return DateFormat.getDateTimeInstance(
                        DateFormat.SHORT, DateFormat.SHORT).format(now);
            case 9:
                return DateFormat.getDateTimeInstance(
                        DateFormat.MEDIUM, DateFormat.SHORT).format(now);
            case 10:
                return DateFormat.getDateTimeInstance(
                        DateFormat.LONG, DateFormat.LONG).format(now);
            default:
                throw new IllegalArgumentException("unsupported date formatting option");
        }
    }

    /**
     * Thread sleep wrapper.
     *
     * @param millis the length of time to sleep in milliseconds
     */
    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }


    /**
     * Keeps the current thread sleeping forever.
     */
    public static void keepAlive() {
        while (true) {
            sleep(100);
        }
    }


    /**
     * Builds xMsg topic of the form: {@code domain:subject:type}.
     *
     * @param domain domain of the message
     * @param subject subject of the message
     * @param type type of the message
     * @return xMsg topic
     * @throws xMsgException
     */
    public static String buildTopic(String domain,
                                    String subject,
                                    String type) throws xMsgException {
        StringBuilder topic = new StringBuilder();
        if (domain == null || domain.equals("*")) {
            throw new xMsgException("domain is not defined");
        }
        topic.append(domain);
        if (subject != null && !subject.equals("*")) {
            topic.append(":").append(subject);
            if (type != null && !type.equals("*")) {
                StringTokenizer st = new StringTokenizer(type, ":");
                while (st.hasMoreTokens()) {
                    String tst = st.nextToken();
                    if (!tst.contains("*")) {
                        topic.append(":").append(tst);
                    } else {
                        break;
                    }
                }
            }
        }
        return topic.toString();
    }

    /**
     * Finds the domain of the xMsg topic.
     * <p>
     * Note that xMsg topic is constructed as: {@code domain:subject:type}.
     *
     * @param topic xMsg topic
     * @return domain of the topic
     * @throws xMsgException in case topic does not have a proper xMsg topic construct
     */
    public static String getTopicDomain(String topic) throws xMsgException {
        return topic.substring(0, topic.indexOf(":"));
    }

    /**
     * Finds the subject of the xMsg topic.
     * <p>
     * Note that xMsg topic is constructed as: {@code domain:subject:type}.
     *
     * @param topic xMsg topic
     * @return subject of the topic. In case subject is missing in the topic, it
     *         returns {@code xMsgConstants.UNDEFINED}.
     * @throws xMsgException in case topic does not have a proper xMsg topic construct
     */
    public static String getTopicSubject(String topic) throws xMsgException {
        StringTokenizer st = new StringTokenizer(topic, ":");
        if (st.hasMoreTokens()) {
            st.nextToken();
        } else {
            throw new xMsgException("malformed xMsg topic.");
        }
        if (st.hasMoreTokens()) {
            return st.nextToken();
        } else {
            return xMsgConstants.UNDEFINED.getStringValue();
        }
    }

    /**
     * Finds the type of the xMsg topic.
     * <p>
     * Note that xMsg topic is constructed as: {@code domain:subject:type}.
     *
     * @param topic xMsg topic
     * @return type of the topic. In case subject is missing in the topic, it
     *         returns xMsgConstants.UNDEFINED.
     *         Note that type can not be defined if subject is not defined,
     *         i.e. xMsg does not support topic such as: {@code domain:*:type}.
     * @throws xMsgException in case topic does not have a proper xMsg topic construct
     */
    public static String getTopicType(String topic) throws xMsgException {
        StringTokenizer st = new StringTokenizer(topic, ":");
        if (st.hasMoreTokens()) {
            st.nextToken();
        } else {
            throw new xMsgException("malformed xMsg topic.");
        }
        if (st.hasMoreTokens()) {
            st.nextToken();
            if (st.hasMoreTokens()) {
                return st.nextToken();
            } else {
                return xMsgConstants.UNDEFINED.getStringValue();
            }
        } else {
            return xMsgConstants.UNDEFINED.getStringValue();
        }
    }

    /**
     * Returns the list of IP addresses of the local node.
     * Useful when the host can have multiple network cards, i.e. IP addresses.
     * <p>
     * This method skip loop-back (127.xxx), ink-local (169.254.xxx),
     * multicast (224.xxx through 238.xxx) and
     * broadcast (255.255.255.255) addresses.
     *
     * @return list of IP addresses
     * @throws SocketException if an I/O error occurs.
     */
    public static List<String> getLocalHostIps() throws SocketException {
        return localHostIps;
    }

    /**
     * Updates the list of IP addresses of the local node.
     * <p>
     * This method skip loop-back (127.xxx), ink-local (169.254.xxx),
     * multicast (224.xxx through 238.xxx) and
     * broadcast (255.255.255.255) addresses.
     *
     * @throws SocketException if an I/O error occurs.
     */
    public static void updateLocalHostIps() throws SocketException {
        List<String> out = new ArrayList<>();
        Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface n = e.nextElement();
            Enumeration<InetAddress> ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress i = ee.nextElement();
                String address = i.getHostAddress();
                if (!(address.startsWith("127") || address.contains(":"))) {
                    out.add(address);
                }
            }
        }
        localHostIps = out;
    }

    /**
     * Returns the IP address of the specified host.
     *
     * @param hostName The name of the host (accepts "localhost")
     * @return dotted notation of the IP address
     * @throws SocketException
     * @throws xMsgException
     */
    public static String toHostAddress(String hostName) throws SocketException, xMsgException {
        if (isIP(hostName)) {
            return hostName;
        }

        if (hostName.equals("localhost")) {
            if (getLocalHostIps().size() > 0) {
                return getLocalHostIps().get(0);
            } else {
                updateLocalHostIps();
                return getLocalHostIps().get(0);
            }
        } else {

            InetAddress address;
            try {
                address = InetAddress.getByName(hostName);
            } catch (UnknownHostException e) {
                throw new xMsgException(e.getMessage());
            }
            return address.getHostAddress();
        }
    }

    /**
     * Checks if the host name is an IPv4 address.
     *
     * @param hostname Host name of the computing node.
     * @return true if host name has an IP form.
     */
    public static boolean isIP(String hostname) {
        Pattern p = Pattern.compile("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}"
                                   + "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");
        Matcher m = p.matcher(hostname);
        return m.find();
    }

    /**
     * Serializes an Object into a protobuf {@link ByteString}.
     *
     * @param object a serializable object
     * @return the serialization of the object as a ByteString
     *         or null in case of error
     */
    public static ByteString serializeToByteString(Object object) {
        if (object instanceof byte[]) {
            return ByteString.copyFrom((byte[]) object);
        } else {
            try (ByteString.Output bs = ByteString.newOutput();
                 ObjectOutputStream out = new ObjectOutputStream(bs)) {
                out.writeObject(object);
                out.flush();
                return bs.toByteString();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    /**
     * Serializes an Object into a byte array.
     *
     * @param object a serializable object
     * @return the serialization of the object as a byte array.
     * @throws IOException if there was an error
     */
    public static byte[] serializeToBytes(Object object)
            throws IOException {
        if (object instanceof byte[]) {
            return (byte[]) object;
        }
        java.io.ByteArrayOutputStream bs = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bs);
        out.writeObject(object);
        out.flush();
        out.close();
        return bs.toByteArray();
    }

    /**
     * Deserializes a protobuf {@link ByteString} into an Object.
     *
     * @param bytes the serialization of the object
     * @return the deserialized Object or null in case of error
     */
    public static Object deserialize(ByteString bytes) {
        byte[] bb = bytes.toByteArray();
        return deserialize(bb);
    }

    /**
     * Deserializes a byte array into an Object.
     *
     * @param bytes the serialization of the object
     * @return the deserialized Object or null in case of error
     */
    public static Object deserialize(byte[] bytes) {
        try (ByteArrayInputStream bs = new ByteArrayInputStream(bytes);
             ObjectInputStream in = new ObjectInputStream(bs)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
