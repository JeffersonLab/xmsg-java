package org.jlab.coda.xmsg.core;

/**
 * <p>
 *     xMsg constants
 * </p>
 *
 * @author gurjyan
 *         Created on 10/6/14
 * @version %I%
 * @since 1.0
 */
public enum xMsgConstants {
    UNDEFINED(0, "undefined"),
    SUCCESS(1, "success"),
    ANY_HOST(2, "*"),

    REGISTRAR(3, "xMsg_Registrar"),

    REGISTER_PUBLISHER(4, "registerPublisher"),
    REGISTER_SUBSCRIBER(5, "registerSubscriber"),
    REGISTER_REQUEST_TIMEOUT(3000,"registerRequestTimeout"),

    REMOVE_PUBLISHER(6, "removePublisherRegistration"),
    REMOVE_SUBSCRIBER(7, "removeSubscriberRegistration"),
    REMOVE_ALL_REGISTRATION(8, "removeAllRegistration"),
    REMOVE_REQUEST_TIMEOUT(3000,"removeRequestTimeout"),

    FIND_REQUEST_TIMEOUT(3000,"findRequestTimeout"),
    FIND_PUBLISHER(9,"findPublisher"),
    FIND_SUBSCRIBER(10,"findSubscribers"),

    BIND(11,"bind"),
    CONNECT(12,"connect"),

    DEFAULT_PORT(7771, "defaultPort"),
    REGISTRAR_PORT(8888, "registrarPort");



    private int intValue;
    private String stringvalue;

    private xMsgConstants(int intValue, String stringValue){
        this.intValue = intValue;
        this.stringvalue = stringValue;
    }

    public int getIntValue() {
        return intValue;
    }
    public String getStringValue() {
        return stringvalue;
    }
}
