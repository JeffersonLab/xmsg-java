package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.data.xMsgD;
import org.jlab.coda.xmsg.excp.xMsgException;

/**
 * <p>
 *     xMsgMessage class defines a message to be serialized and sent.
 *     Uses xMsgData class generated as a result of the proto-buffer
 *     description.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 11/5/14
 */
public class xMsgMessage {

    /**
     * Message address section
     */
    private String author = xMsgConstants.UNDEFINED.getStringValue();
    private String domain = xMsgConstants.UNDEFINED.getStringValue();
    private String subject = xMsgConstants.UNDEFINED.getStringValue();
    private String type = xMsgConstants.UNDEFINED.getStringValue();
    /**
     * Message data section
     */
    private xMsgD.Data data;

    public xMsgMessage(String author,
                       String domain,
                       String subject,
                       String type,
                       Object data) throws xMsgException {
        this.author = author;
        this.domain = domain;
        this.subject = subject;
        this.type = type;

        if (data instanceof xMsgD.Data){
            this.data = (xMsgD.Data)data;
        } else {
            this.data = _createData(data);
        }
    }


    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public xMsgD.Data getData() {
        return data;
    }

    public void setData(xMsgD.Data data) {
        this.data = data;
    }

    private xMsgD.Data _createData(Object d) throws xMsgException {
        xMsgD.Data.Builder trb = xMsgD.Data.newBuilder();

        trb.setAuthor(author);
        trb.setId(0);
        trb.setDataDescription(xMsgConstants.UNDEFINED.getStringValue());

        if(d instanceof Integer){
            Integer in_data = (Integer)d;
            trb.setXtype(xMsgD.Data.DataType.T_FLSINT32);
            trb.setFLSINT32(in_data);

        } else if (d instanceof Integer[]){
            Integer[] in_data = (Integer[])d;
            trb.setXtype(xMsgD.Data.DataType.T_FLSINT32A);
            for(int id:in_data) trb.addFLSINT32A(id);

        } else if (d instanceof Float){
            Float in_data = (Float)d;
            trb.setXtype(xMsgD.Data.DataType.T_FLOAT);
            trb.setFLOAT(in_data);

        } else if (d instanceof Float[]){
            Float[] in_data = (Float[])d;
            trb.setXtype(xMsgD.Data.DataType.T_FLOATA);
            for(float id:in_data) trb.addFLOATA(id);

        } else if (d instanceof Double){
            Double in_data = (Double)d;
            trb.setXtype(xMsgD.Data.DataType.T_DOUBLE);
            trb.setDOUBLE(in_data);

        } else if (d instanceof Double[]){
            Double[] in_data = (Double[])d;
            trb.setXtype(xMsgD.Data.DataType.T_DOUBLEA);
            for(double id:in_data) trb.addDOUBLEA(id);

        } else if (d instanceof String){
            String in_data = (String)d;
            trb.setXtype(xMsgD.Data.DataType.T_STRING);
            trb.setSTRING(in_data);

        } else if (d instanceof String[]){
            String[] in_data = (String[])d;
            trb.setXtype(xMsgD.Data.DataType.T_STRINGA);
            for(String id:in_data) trb.addSTRINGA(id);

        } else {
            throw new xMsgException("Unsupported data type");
        }

        return trb.build();

    }

    @Override
    public String toString() {
        return "xMsgMessage{" +
                "author='" + author + '\'' +
                ", domain='" + domain + '\'' +
                ", subject='" + subject + '\'' +
                ", type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}
