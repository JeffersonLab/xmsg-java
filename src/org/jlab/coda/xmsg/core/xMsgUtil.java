package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.excp.xMsgSubscribingException;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.util.*;

/**
 * <p>
 *     xMsg utility class
 * </p>
 *
 * @author gurjyan
 *         Created on 10/10/14
 * @version %I%
 * @since 1.0
 */

public class xMsgUtil {

    /**
     * <p>
     *     Returns formatted string of the current date and time:
     * </p>
     *
     * @param type int that defines the option of the formatting.
     *             Options 1 through 10 are accepted, producing following formatting:
     *             <p>
     *             <ul>
     *              <li>Tue Nov 04 20:14:11 EST 2003</li>
     *              <li>11/4/03 8:14 PM</li>
     *              <li>8:14:11 PM</li>
     *              <li>Nov 4, 2003 8:14:11 PM</li>
     *              <li>8:14 PM</li>
     *              <li>8:14:11 PM</li>
     *              <li>8:14:11 PM EST</li>
     *              <li>11/4/03 8:14 PM</li>
     *              <li>Nov 4, 2003 8:14 PM</li>
     *              <li>November 4, 2003 8:14:11 PM EST</li>
     *             </ul>
     *             </p>
     * @return formatted string of the current data
     */
    public static String currentTime(int type){

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
        }
        return "unsupported date formatting option";
    }

    /**
     * <p>
     *     Thread sleep wrapper
     * </p>
     * @param t in milli seconds
     */
    public static void sleep(int t){
        try {
            Thread.sleep(t);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * <p>
     *     Sleeps for ever
     * </p>
     * @param t in milli seconds
     */
    public static void sleep_fe(int t){
        while(true) {
            sleep(t);
        }
    }

    public static void keepAlive(){
        sleep_fe(7);
    }

    /**
     * <p>
     *     Returns the IP address of the specified host
     * </p>
     *
     * @param hostName The name of the host (accepts "localhost")
     * @return dotted notation of the IP address
     * @throws xMsgException
     */
    public static String host_to_ip(String hostName) throws xMsgException, SocketException {

        if(hostName.equals("localhost")){
            return getLocalHostIps().get(0);
        }

        InetAddress address;
        try {
            address = InetAddress.getByName(hostName);
        } catch (UnknownHostException e) {
            throw new xMsgException(e.getMessage());
        }
        return address.getHostAddress();
    }

    /**
     * <p>
     *     Builds xMsg topic of the form:
     *     domain:subject:type
     * </p>
     * @param domain domain of the message
     * @param subject subject of the message
     * @param type type of the message
     * @return xMsg topic
     * @throws xMsgSubscribingException
     */
    public static String buildTopic(String domain,
                                    String subject,
                                    String type) throws xMsgException {
        StringBuilder topic = new StringBuilder();
        if(domain==null || domain.equals("*")) throw new xMsgException("domain is not defined");
        topic.append(domain);
        if(subject!=null && !subject.equals("*")) {
            topic.append(":").append(subject);
            if(type!=null && !type.equals("*")) {
                StringTokenizer st = new StringTokenizer(type,":");
                while(st.hasMoreTokens()){
                    String tst = st.nextToken();
                    if(!tst.contains("*")) {
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
     * <p>
     *  Finds the domain of the xMsg topic.
     *  Note that xMsg topic is constructed as:
     *  domain:subject:type
     *
     * </p>
     * @param topic xMsg topic
     * @return domain of the topic
     * @throws xMsgException in case topic does
     * not have a proper xMsg topic construct
     */
    public static String getTopicDomain(String topic) throws xMsgException {
        StringTokenizer st = new StringTokenizer(topic,":");
        if(st.hasMoreTokens())return st.nextToken();
        else throw new xMsgException("malformed xMsg topic.");
    }

    /**
     * <p>
     *  Finds the subject of the xMsg topic.
     *  Note that xMsg topic is constructed as:
     *  domain:subject:type
     *
     * </p>
     * @param topic xMsg topic
     *
     * @return subject of the topic. In case
     * subject is missing in the topic, it
     * returns xMsgConstants.UNDEFINED.
     *
     * @throws xMsgException in case topic does
     * not have a proper xMsg topic construct
     */
    public static String getTopicSubject(String topic) throws xMsgException {
        StringTokenizer st = new StringTokenizer(topic,":");
        if(st.hasMoreTokens()) st.nextToken();
        else throw new xMsgException("malformed xMsg topic.");
        if(st.hasMoreTokens()) return st.nextToken();
        else return xMsgConstants.UNDEFINED.getStringValue();
    }

    /**
     * <p>
     *  Finds the type of the xMsg topic.
     *  Note that xMsg topic is constructed as:
     *  domain:subject:type
     *
     * </p>
     * @param topic xMsg topic
     *
     * @return type of the topic. In case
     * subject is missing in the topic, it
     * returns xMsgConstants.UNDEFINED.
     * Note that type can not be defined if
     * subject is not defined, i.e. xMsg
     * does not support topic such as:
     * domain:*:type
     *
     * @throws xMsgException in case topic does
     * not have a proper xMsg topic construct
     */
    public static String getTopicType(String topic) throws xMsgException {
        StringTokenizer st = new StringTokenizer(topic,":");
        if(st.hasMoreTokens()) st.nextToken();
        else throw new xMsgException("malformed xMsg topic.");
        if(st.hasMoreTokens()) {
            st.nextToken();
            if (st.hasMoreTokens()) return st.nextToken();
            else return xMsgConstants.UNDEFINED.getStringValue();
        } else return xMsgConstants.UNDEFINED.getStringValue();
    }

    /**
     * <p>
     *     Returns list of IP addresses of a node, that can have
     *     multiple network cards, i.e. IP addresses. This method
     *     skip loop-back (127.xxx), ink-local (169.254.xxx),
     *     multicast (224.xxx through 238.xxx) and
     *     broadcast (255.255.255.255) addresses.
     * </p>
     *
     * @return List of IP addresses
     * @throws SocketException
     */
    public static List<String> getLocalHostIps() throws SocketException {
        List<String> out = new ArrayList<>();
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        while(e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements())
            {
                InetAddress i = (InetAddress) ee.nextElement();
                String address = i.getHostAddress();
                if(address.startsWith("127") || address.contains(":")){
                } else {
                    out.add(address);
                }
            }
        }
        return out;
    }

    public static xMsgD.Data.Builder createxMsgData(String author,
                                            int com_id,
                                            String description,
                                            Object d) throws xMsgException {
        xMsgD.Data.Builder trb = xMsgD.Data.newBuilder();

        trb.setDataAuthor(author);
        trb.setId(com_id);
        trb.setDataDescription(description);

        if(d instanceof Integer){
            Integer in_data = (Integer)d;
            trb.setDataType(xMsgD.Data.DType.T_FLSINT32);
            trb.setFLSINT32(in_data);

        } else if (d instanceof Integer[]){
            Integer[] in_data = (Integer[])d;
            trb.setDataType(xMsgD.Data.DType.T_FLSINT32A);
            for(int id:in_data) trb.addFLSINT32A(id);

        } else if (d instanceof Float){
            Float in_data = (Float)d;
            trb.setDataType(xMsgD.Data.DType.T_FLOAT);
            trb.setFLOAT(in_data);

        } else if (d instanceof Float[]){
            Float[] in_data = (Float[])d;
            trb.setDataType(xMsgD.Data.DType.T_FLOATA);
            for(float id:in_data) trb.addFLOATA(id);

        } else if (d instanceof Double){
            Double in_data = (Double)d;
            trb.setDataType(xMsgD.Data.DType.T_DOUBLE);
            trb.setDOUBLE(in_data);

        } else if (d instanceof Double[]){
            Double[] in_data = (Double[])d;
            trb.setDataType(xMsgD.Data.DType.T_DOUBLEA);
            for(double id:in_data) trb.addDOUBLEA(id);

        } else if (d instanceof String){
            String in_data = (String)d;
            trb.setDataType(xMsgD.Data.DType.T_STRING);
            trb.setSTRING(in_data);

        } else if (d instanceof String[]){
            String[] in_data = (String[])d;
            trb.setDataType(xMsgD.Data.DType.T_STRINGA);
            for(String id:in_data) trb.addSTRINGA(id);

        } else {
            throw new xMsgException("Unsupported data type");
        }

        return trb;

    }


}
