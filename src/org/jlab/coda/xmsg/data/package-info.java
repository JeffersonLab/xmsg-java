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

/**
 * Data classes for pub/sub messages and registration requests.
 * xMsg uses <a href="https://developers.google.com/protocol-buffers/">Protocol
 * Buffers</a> to generate and serialize its internal data classes.
 * <p>
 * The {@link org.jlab.coda.xmsg.data.xMsgM.xMsgMeta xMsgMeta} class is used to
 * store extra information about a {@link org.jlab.coda.xmsg.core.xMsgMessage
 * message} and its data. Although the message class requires the data to be a
 * binary byte array, and the serialization is left to be handled by the
 * applications, the {@link org.jlab.coda.xmsg.data.xMsgD.xMsgData xMsgData}
 * class is provided as a quick container to send and serialize values of basic
 * types data between xMsg actors, working for all supported languages. The
 * default mime-types are listed in {@link org.jlab.coda.xmsg.data.xMsgMimeType
 * xMsgMimeType}.
 * <p>
 * The {@link org.jlab.coda.xmsg.data.xMsgRegInfo xMsgRegInfo} is used
 * to handle the registration of an actor with a
 * {@link org.jlab.coda.xmsg.sys.xMsgRegistrar registrar server},
 * and the {@link org.jlab.coda.xmsg.data.xMsgRegQuery xMsgRegQuery}
 * class helps creating a query to a registrar for the specified matching
 * actors.
 * The registration data is returned as
 * {@link org.jlab.coda.xmsg.data.xMsgR.xMsgRegistration xMsgRegistration}
 * objects, but the {@link org.jlab.coda.xmsg.data.xMsgRegRecord xMsgRegRecord}
 * wrapper is provided to simplify accessing the different fields.
 */
package org.jlab.coda.xmsg.data;
