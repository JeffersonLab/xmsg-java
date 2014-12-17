package org.jlab.coda.xmsg.net;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.excp.xMsgException;

import static org.jlab.coda.xmsg.core.xMsgUtil.host_to_ip;

/**
 *<p>
 *    xMsg network address container class.
 *    Defines a key constructed as host:port (xMsg
 *    convention) for storing xMsgConnection objects.
 *</p>
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public class xMsgAddress {

    private String host = xMsgConstants.UNDEFINED.getStringValue();
    private int port = xMsgConstants.DEFAULT_PORT.getIntValue();
    private String key = xMsgConstants.UNDEFINED.getStringValue();

    /**
     * <p>
     *     Constructor that converts host name into a
     *     dotted notation of the IP address.
     *     This constructor uses xMsg default port
     * </p>
     * @param host name
     * @throws xMsgException
     */
    public xMsgAddress(String host) throws xMsgException {
        this.host = host_to_ip(host);
        key = this.host+":"+this.port;
    }

    /**
     * <p>
     *     Constructor that creates an instance of the
     *     cMsgAddress using user provided host and port
     * </p>
     * @param host name
     * @param port port number
     * @throws xMsgException
     */
    public xMsgAddress(String host, int port) throws xMsgException {
        this.host = host_to_ip(host);
        this.port = port;
        key = this.host+":"+this.port;
    }

    /**
     * Returns the host name
     * @return hostname
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the port number
     * @return port number
     */
    public int getPort() {
        return port;
    }

    /**
     * <p>
     *     Allows to change the port number.
     *     This method should be used with caution, making sure
     *     that the xMsgConnection associated with this address
     *     is actually created using this new port.
     * </p>
     * @param port port number
     */
    public void setPort(int port) {
        this.port = port;
        key = this.host+":"+this.port;
    }

    /**
     * Returns xMsg address key, constructed asd host:ort
     * @return address key
     */
    public String getKey() {
        return key;
    }
}
