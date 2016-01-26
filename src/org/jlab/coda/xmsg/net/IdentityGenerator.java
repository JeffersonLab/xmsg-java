package org.jlab.coda.xmsg.net;

import org.jlab.coda.xmsg.core.xMsgUtil;

import java.util.Random;

final class IdentityGenerator {

    private IdentityGenerator() { }

    // CHECKSTYLE.OFF: ConstantName
    private static final Random randomGenerator = new Random();
    private static final long ctrlIdPrefix = getCtrlIdPrefix();
    // CHECKSTYLE.ON: ConstantName

    private static long getCtrlIdPrefix() {
        final int javaId = 1;
        final int ipHash = xMsgUtil.localhost().hashCode() & Integer.MAX_VALUE;
        return javaId * 100000000 + (ipHash % 1000) * 100000;
    }

    static String getCtrlId() {
        return Long.toString(ctrlIdPrefix + randomGenerator.nextInt(100000));
    }
}
