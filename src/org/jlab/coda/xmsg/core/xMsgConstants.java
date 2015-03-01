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
    ANY(2, "*"),

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

    INFO(11,"INFO"),
    WARNING(12,"WARNING"),
    ERROR(13,"ERROR"),
    DONE(14,"done"),

    NO_RESULT(15,"none"),

    BIND(16,"bind"),
    CONNECT(17,"connect"),

    DEFAULT_PORT(7771, "defaultPort"),
    REGISTRAR_PORT(8888, "registrarPort"),

    // Note: xMsg envelope passed through zmq
    // consists of three strings:
    // 1) topic
    // 2) dataType (accepts two types defined below
    // 3) actual serialized data
    ENVELOPE_DATA_TYPE_STRING(18,"string"),
    ENVELOPE_DATA_TYPE_XMSGDATA(19,"xMsgData");

    private int intValue;
    private String stringValue;

    private xMsgConstants(int intValue, String stringValue){
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    public int getIntValue() {
        return intValue;
    }
    public String getStringValue() {
        return stringValue;
    }
}
