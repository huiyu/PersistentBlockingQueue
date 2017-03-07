package io.github.huiyu.collect;

public interface Serializer<T> {

    byte[] encode(T object);

    T decode(byte[] data);
}
