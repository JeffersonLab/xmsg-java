/*
 *    Copyright (C) 2021. Jefferson Lab (JLAB). All Rights Reserved.
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


import org.jlab.coda.xmsg.excp.xMsgException;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;



/**
 * The user-data message for xMsg point-to-point communications.
 * <p>
 * Currently a p2p xMsg message is composed of only binary data.
 * The data byte array contains the binary representation of the actual data of
 * the message. Complex objects must be serialized before creating the message
 * (i.e. applications using xMsg must take care of the binary data format).
 * <p>
 * This is designed to send evio data which has it own embedded metadata,
 * including the endianness.
 *
 * @version 2.x
 */
public class xMsgMessagePtp {

    private byte[] data;

    /**
     * Constructs a new message.
     * <p>
     * The byte array will be owned by the message, thus, it cannot be modified
     * by the calling code after creating this message.
     * If the byte array must be reused or modified, pass the array to the
     * constructor as {@code data.clone()} to ensure the message keeps a copy of
     * the data and not the original array.
     *
     * @param data serialized data
     */
    public xMsgMessagePtp(byte[] data) {this.data = data;}


    /**
     * Creates a message from the 0MQ message received from the wire.
     * @param msg the received 0MQ message
     */
    xMsgMessagePtp(ZMsg msg) throws xMsgException {

        if (msg.size() != 1) {
            throw new xMsgException("invalid point-to-point message format");
        }

        ZFrame dataFrame = msg.pop();
        this.data = dataFrame.getData();
    }

    /**
     * Set the data in this message.
     * @param msg containing serialized data.
     */
    void setData(ZMsg msg) {
        this.data = msg.getFirst().getData();
    }

    /**
     * Set the data in this message.
     * @param data serialized data.
     */
    void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Serializes this message into a 0MQ message,
     * ready to send it over the wire.
     * @return the 0MQ message
     */
    ZMsg serialize() {
        ZMsg msg = new ZMsg();
        msg.add(data);
        return msg;
    }

    /**
     * Returns the size of the byte array containing the data.
     * @return the size of the data, in bytes
     */
    public int getDataSize() {
        return data != null ? data.length : 0;
    }

    /**
     * Returns the data of the message.
     * @return the byte array with the raw message data
     */
    public byte[] getData() {
        return data;
    }

}
