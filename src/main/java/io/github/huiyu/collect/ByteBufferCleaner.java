package io.github.huiyu.collect;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

class ByteBufferCleaner {

    private static final boolean CLEAN_SUPPORTED;
    private static final Method METHOD_GET_CLEANER;
    private static final Method METHOD_CLEAN;

    static {
        Method methodGetCleaner = null;
        Method methodClean = null;
        boolean cleanSupported;
        try {
            methodGetCleaner = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
            methodGetCleaner.setAccessible(true);
            methodClean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            methodClean.setAccessible(true);
            cleanSupported = true;
        } catch (Exception e) {
            cleanSupported = false;
        }
        CLEAN_SUPPORTED = cleanSupported;
        METHOD_GET_CLEANER = methodGetCleaner;
        METHOD_CLEAN = methodClean;
    }

    public static void clean(ByteBuffer buffer) {
        if (CLEAN_SUPPORTED && buffer.isDirect()) {
            try {
                Object cleaner = METHOD_GET_CLEANER.invoke(buffer);
                METHOD_CLEAN.invoke(cleaner);
            } catch (Exception e) {
                // DO NOTHING
            }
        }
    }
}
