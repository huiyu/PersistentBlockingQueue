package io.github.huiyu.collect;

public class Preconditions {

    public static <T> T checkNotNull(T t) {
        if (t == null)
            throw new NullPointerException();
        return t;
    }

    public static <T> T checkNotNull(T t, String msg) {
        if (t == null)
            throw new NullPointerException(msg);
        return t;
    }

    public static void checkArgument(boolean expr) {
        if (!expr)
            throw new IllegalArgumentException();
    }

    public static void checkArgument(boolean expr, String msg) {
        if (!expr)
            throw new IllegalArgumentException(msg);
    }
}
