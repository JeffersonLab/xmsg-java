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

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.xsys.xMsgProxy;
import org.zeromq.ZContext;

class ProxyThread {

    private final ZContext context = new ZContext();
    private final Thread proxyThread;

    ProxyThread() {
        proxyThread = xMsgUtil.newThread("proxy-thread", () -> {
            ZContext shadow = ZContext.shadow(context);
            try {
                xMsgProxy proxy = new xMsgProxy(shadow);
                proxy.start();
            } catch (xMsgException e) {
                e.printStackTrace();
            } finally {
                shadow.destroy();
            }
        });
    }

    public void start() {
        proxyThread.start();
        xMsgUtil.sleep(100);
    }

    public void stop() throws InterruptedException {
        context.destroy();
        proxyThread.interrupt();
        proxyThread.join(1000);
    }
}
