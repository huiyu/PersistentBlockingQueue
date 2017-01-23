package me.jeffreyu.collect;

public interface Serializer<T> {

    byte[] encode(T object);

    T decode(byte[] data);
}
