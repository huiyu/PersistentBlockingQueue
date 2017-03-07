package io.github.huiyu.collect;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public final class Serializers {

    public static final Serializer<Byte> BYTE_SERIALIZER = new Serializer<Byte>() {

        @Override
        public byte[] encode(Byte object) {
            return new byte[]{object};
        }

        @Override
        public Byte decode(byte[] data) {
            return data[0];
        }
    };

    public static final Serializer<Short> SHORT_SERIALIZER = new Serializer<Short>() {

        @Override
        public byte[] encode(Short object) {
            ByteBuffer buf = ByteBuffer.allocate(2);
            buf.putShort(object);
            return buf.array();
        }

        @Override
        public Short decode(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            return buf.getShort();
        }
    };

    public static final Serializer<Integer> INTEGER_SERIALIZER = new Serializer<Integer>() {

        @Override
        public byte[] encode(Integer object) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(object);
            return buf.array();
        }

        @Override
        public Integer decode(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            return buf.getInt();
        }
    };


    public static final Serializer<Long> LONG_SERIALIZER = new Serializer<Long>() {

        @Override
        public byte[] encode(Long object) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putLong(object);
            return buf.array();
        }

        @Override
        public Long decode(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            return buf.getLong();
        }
    };

    public static final Serializer<Boolean> BOOLEAN_SERIALIZER = new Serializer<Boolean>() {

        private final Byte TRUE = 1;
        private final Byte FALSE = 0;

        @Override
        public byte[] encode(Boolean object) {
            byte b = object ? TRUE : FALSE;
            return BYTE_SERIALIZER.encode(b);
        }

        @Override
        public Boolean decode(byte[] data) {
            Byte decode = BYTE_SERIALIZER.decode(data);
            return !decode.equals(FALSE);
        }
    };

    public static final Serializer<Character> CHARACTER_SERIALIZER = new Serializer<Character>() {

        @Override
        public byte[] encode(Character object) {
            ByteBuffer buf = ByteBuffer.allocate(2);
            buf.putChar(object);
            return buf.array();
        }

        @Override
        public Character decode(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            return buf.getChar();
        }
    };

    public static final Serializer<Float> FLOAT_SERIALIZER = new Serializer<Float>() {

        @Override
        public byte[] encode(Float object) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putFloat(object);
            return buf.array();
        }

        @Override
        public Float decode(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            return buf.getFloat();
        }
    };

    public static final Serializer<Double> DOUBLE_SERIALIZER = new Serializer<Double>() {

        @Override
        public byte[] encode(Double object) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putDouble(object);
            return buf.array();
        }

        @Override
        public Double decode(byte[] data) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            return buf.getDouble();
        }
    };

    public static final Serializer<String> STRING_SERIALIZER = new Serializer<String>() {

        private final Charset UTF_8 = Charset.forName("UTF-8");

        @Override
        public byte[] encode(String object) {
            return object.getBytes(UTF_8);
        }

        @Override
        public String decode(byte[] data) {
            return new String(data, UTF_8);
        }
    };

    public static final Serializer<byte[]> BYTE_ARRAY_SERIALIZER = new Serializer<byte[]>() {

        @Override
        public byte[] encode(byte[] object) {
            return object;
        }

        @Override
        public byte[] decode(byte[] data) {
            return data;
        }
    };

    public static final Serializer<Object> OBJECT_SERIALIZER = new Serializer<Object>() {

        @Override
        public byte[] encode(Object object) {
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(bao)) {
                oos.writeObject(object);
                return bao.toByteArray();
            } catch (IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public Object decode(byte[] data) {
            ByteArrayInputStream bai = new ByteArrayInputStream(data);
            try (ObjectInputStream ois = new ObjectInputStream(bai)) {
                return ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new SerializationException(e);
            }
        }
    };
}
