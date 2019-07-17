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

package org.jlab.coda.xmsg.core;

import com.google.protobuf.ByteString;

import org.jlab.coda.xmsg.sys.util.ThreadUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * xMsg utility methods.
 *
 * @since 2.x
 */
public final class xMsgUtil {

    private static List<String> localHostIps = new ArrayList<>();

    private static final int REPLY_TO_SEQUENCE_SIZE = 1000000;

    private static final Random randomGenerator; // nocheck: ConstantName
    private static final AtomicInteger replyToGenerator; // nocheck: ConstantName

    static {
        randomGenerator = new Random();
        replyToGenerator = new AtomicInteger(randomGenerator.nextInt(REPLY_TO_SEQUENCE_SIZE));
    }

    private xMsgUtil() { }

    /**
     * Thread sleep wrapper.
     *
     * @param millis the length of time to sleep in milliseconds
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // nothing
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
     * Returns the localhost IPv4.
     *
     * In case of multiple network cards, this method will return the first one
     * in the list of addresses of all network cards, with {@code
     * InetAddress.getLocalHost()} being preferred.
     * <p>
     * Fallbacks to the loopback address if no other address was found.
     *
     * @return a string with the localhost IPv4
     * @throws UncheckedIOException if an I/O error occurs.
     */
    public static String localhost() {
        return toHostAddress("localhost");
    }

    /**
     * Returns the list of IPv4 addresses of the local node.
     * Useful when the host has multiple network cards.
     * <p>
     * The address returned by {@code InetAddress.getLocalHost()} goes first.
     * Then all non-loopback site-local addresses, and finally all other
     * non-loopback addresses.
     * <p>
     * Fallbacks to the loopback address if no other address was found.
     *
     * @return list of IP addresses
     * @throws UncheckedIOException if an I/O error occurs.
     */
    public static List<String> getLocalHostIps() {
        if (localHostIps.isEmpty()) {
            updateLocalHostIps();
        }
        return localHostIps;
    }

    /**
     * Updates the list of IPv4 addresses of the local node.
     * <p>
     * The address returned by {@code InetAddress.getLocalHost()} goes first.
     * Then all non-loopback site-local addresses, and finally all other
     * non-loopback addresses.
     * <p>
     * Fallbacks to the loopback address if no other address was found.
     *
     * @throws UncheckedIOException if an I/O error occurs.
     */
    public static void updateLocalHostIps() {
        Set<String> ips = new LinkedHashSet<>();

        try {
            // prefer address returned by getLocalHost first, if any
            InetAddress local = InetAddress.getLocalHost();
            if (local instanceof Inet4Address && !local.isLoopbackAddress()) {
                ips.add(local.getHostAddress());
            }
        } catch (IOException e) {
            // ignore errors to try all network interfaces for an address
        }

        try {
            Set<String> localIps = new LinkedHashSet<>();
            Set<String> nonLocalIps = new LinkedHashSet<>();

            InetAddress loopback = null;

            // get all non-loopback addresses, if any
            Enumeration<NetworkInterface> allIfaces = NetworkInterface.getNetworkInterfaces();
            while (allIfaces.hasMoreElements()) {
                NetworkInterface iface = allIfaces.nextElement();
                if (!iface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> allAddr = iface.getInetAddresses();
                while (allAddr.hasMoreElements()) {
                    InetAddress addr = allAddr.nextElement();
                    if (addr instanceof Inet4Address) {
                        if (addr.isLoopbackAddress()) {
                            if (loopback == null) {
                                loopback = addr;
                            }
                            continue;
                        }
                        if (addr.isSiteLocalAddress()) {
                            localIps.add(addr.getHostAddress());
                        } else {
                            nonLocalIps.add(addr.getHostAddress());
                        }
                    }
                }
            }

            ips.addAll(localIps);
            ips.addAll(nonLocalIps);

            // no non-loopback addresses found, default to loopback or throw an error
            if (ips.isEmpty()) {
                if (loopback != null) {
                    ips.add(loopback.getHostAddress());
                } else {
                    throw new IOException("no IPv4 address found");
                }
            }

            localHostIps = new ArrayList<>(ips);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Determines the IP address of the specified host.
     *
     * In case of multiple network cards in the local host, this method will
     * return the first one in the list of addresses of all network cards,
     * with {@code InetAddress.getLocalHost()} being preferred.
     *
     * @param hostName The name of the host (accepts "localhost")
     * @return dotted notation of the IPv4 address
     * @throws UncheckedIOException if the hostname could not be resolved
     */
    public static String toHostAddress(String hostName) {
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
            try {
                return InetAddress.getByName(hostName).getHostAddress();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
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
     * Checks if the argument is a valid IP address.
     *
     * @param address the address to be validated
     * @return the given address
     * @throws IllegalArgumentException if the address is null or not a valid IP
     */
    public static String validateIP(String address) {
        if (address == null) {
            throw new IllegalArgumentException("null IP address");
        }
        if (!isIP(address)) {
            throw new IllegalArgumentException("invalid IP address: " + address);
        }
        return address;
    }

    static String getUniqueReplyTo(String subject) {
        long next = replyToGenerator.getAndIncrement() & 0xffffffffL;
        int id = (int) (next % REPLY_TO_SEQUENCE_SIZE + REPLY_TO_SEQUENCE_SIZE);
        return "ret:" + subject + ":" + id;
    }

    // for testing
    static void setUniqueReplyToGenerator(int value) {
        replyToGenerator.set(value);
    }

    static String encodeIdentity(String address, String name) {
        String id = address + "#" + name + "#" + randomGenerator.nextInt(100);
        int idHash = id.hashCode() & Integer.MAX_VALUE;
        int minValue = 0x1000_0000;
        if (idHash < minValue) {
            idHash += minValue;
        }
        return Integer.toHexString(idHash);
    }

    /**
     * Serializes an Object into a protobuf {@link ByteString}.
     *
     * @param object a serializable object
     * @return the serialization of the object as a ByteString
     * @throws IOException if there was an error
     */
    public static ByteString serializeToByteString(Object object) throws IOException {
        if (object instanceof byte[]) {
            return ByteString.copyFrom((byte[]) object);
        } else {
            try (ByteString.Output bs = ByteString.newOutput();
                 ObjectOutputStream out = new ObjectOutputStream(bs)) {
                out.writeObject(object);
                out.flush();
                return bs.toByteString();
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
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bs)) {
            out.writeObject(object);
            out.flush();
            return bs.toByteArray();
        }
    }

    /**
     * De-serializes a protobuf {@link ByteString} into an Object.
     *
     * @param bytes the serialization of the object
     * @return the deserialized Object
     * @throws ClassNotFoundException if class of a serialized object cannot be found.
     * @throws IOException if there was an error
     */
    public static Object deserialize(ByteString bytes)
            throws ClassNotFoundException, IOException {
        byte[] bb = bytes.toByteArray();
        return deserialize(bb);
    }

    /**
     * De-serializes a byte array into an Object.
     *
     * @param bytes the serialization of the object
     * @return the deserialized Object
     * @throws ClassNotFoundException if class of a serialized object cannot be found.
     * @throws IOException if there was an error
     */
    public static Object deserialize(byte[] bytes)
            throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bs = new ByteArrayInputStream(bytes);
             ObjectInputStream in = new ObjectInputStream(bs)) {
            return in.readObject();
        }
    }

    /**
     * Creates a new Thread that reports uncaught exceptions.
     *
     * @param name the name for the thread
     * @param target the object whose run method is invoked when this thread is started
     * @return a Thread object that will run the target
     */
    public static Thread newThread(String name, Runnable target) {
        return ThreadUtils.newThread(name, target);
    }

    /**
     * Creates a new ThreadPoolExecutor.
     *
     * @param maxThreads the maximum number of threads
     * @param namePrefix the prefix for the name of the threads
     * @return the created xMsg custom thread pool executor
     */
    public static ThreadPoolExecutor newThreadPool(int maxThreads, String namePrefix) {
        return ThreadUtils.newThreadPool(maxThreads, namePrefix, new LinkedBlockingQueue<>());
    }

    /**
     * Creates a new ThreadPoolExecutor with a user controlled queue.
     *
     * @param maxThreads the maximum number of threads
     * @param namePrefix the prefix for the name of the threads
     * @param workQueue the queue to hold waiting tasks
     * @return the created xMsg custom thread pool executor
     */
    public static ThreadPoolExecutor newThreadPool(int maxThreads,
                                                   String namePrefix,
                                                   BlockingQueue<Runnable> workQueue) {
        return ThreadUtils.newThreadPool(maxThreads, namePrefix, workQueue);
    }
}
