package org.jlab.coda.xmsg.core;

import org.jlab.coda.xmsg.data.xMsgD.xMsgData;
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
    private xMsgData data;

    public xMsgMessage(String author,
                       String domain,
                       String subject,
                       String type,
                       Object data) throws xMsgException {
        this.author = author;
        this.domain = domain;
        this.subject = subject;
        this.type = type;

        if (data instanceof xMsgData){
            this.data = (xMsgData)data;
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

    public xMsgData getData() {
        return data;
    }

    public void setData(xMsgData data) {
        this.data = data;
    }

    private xMsgData _createData(Object d) throws xMsgException {
        xMsgData.Builder trb = xMsgData.newBuilder();

        trb.setAuthour(author);
        trb.setId(0);
        trb.setDataDescription(xMsgConstants.UNDEFINED.getStringValue());

        if(d instanceof Integer){
            Integer in_data = (Integer)d;
            trb.setType(xMsgData.DataType.FLSINT32);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            t_data.setFLSINT32(in_data);
            trb.setData(t_data);

        } else if (d instanceof Integer[]){
            Integer[] in_data = (Integer[])d;
            trb.setType(xMsgData.DataType.FLSINT32A);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            for(int id:in_data) t_data.addFLSINT32A(id);
            trb.setData(t_data);

        } else if (d instanceof Float){
            Float in_data = (Float)d;
            trb.setType(xMsgData.DataType.FLOAT);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            t_data.setFLOAT(in_data);
            trb.setData(t_data);

        } else if (d instanceof Float[]){
            Float[] in_data = (Float[])d;
            trb.setType(xMsgData.DataType.FLOATA);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            for(float id:in_data) t_data.addFLOATA(id);
            trb.setData(t_data);

        } else if (d instanceof Double){
            Double in_data = (Double)d;
            trb.setType(xMsgData.DataType.DOUBLE);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            t_data.setDOUBLE(in_data);
            trb.setData(t_data);

        } else if (d instanceof Double[]){
            Double[] in_data = (Double[])d;
            trb.setType(xMsgData.DataType.DOUBLEA);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            for(double id:in_data) t_data.addDOUBLEA(id);
            trb.setData(t_data);

        } else if (d instanceof String){
            String in_data = (String)d;
            trb.setType(xMsgData.DataType.STRING);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            t_data.setSTRING(in_data);
            trb.setData(t_data);

        } else if (d instanceof String[]){
            String[] in_data = (String[])d;
            trb.setType(xMsgData.DataType.STRINGA);
            xMsgData.Data.Builder t_data = xMsgData.Data.newBuilder();
            for(String id:in_data) t_data.addSTRINGA(id);
            trb.setData(t_data);

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
