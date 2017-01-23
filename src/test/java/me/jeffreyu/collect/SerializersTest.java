package me.jeffreyu.collect;

import org.junit.Test;

import java.io.Serializable;
import java.util.Objects;

import static org.junit.Assert.*;

public class SerializersTest {

    @Test
    public void testByteSerializer() {
        Byte b = 1;
        byte[] encode = Serializers.BYTE_SERIALIZER.encode(b);
        assertEquals(1, encode.length);
        Byte decode = Serializers.BYTE_SERIALIZER.decode(encode);
        assertEquals(b, decode);
    }

    @Test
    public void testShortSerializer() {
        Short s = 1;
        byte[] encode = Serializers.SHORT_SERIALIZER.encode(s);
        assertEquals(2, encode.length);
        Short decode = Serializers.SHORT_SERIALIZER.decode(encode);
        assertEquals(s, decode);
    }

    @Test
    public void testIntegerSerializer() {
        Integer i = Integer.MAX_VALUE;
        byte[] encode = Serializers.INTEGER_SERIALIZER.encode(i);
        assertEquals(4, encode.length);
        Integer decode = Serializers.INTEGER_SERIALIZER.decode(encode);
        assertEquals(i, decode);
    }

    @Test
    public void testLongSerializer() {
        Long l = Long.MAX_VALUE;
        byte[] encode = Serializers.LONG_SERIALIZER.encode(l);
        assertEquals(8, encode.length);
        Long decode = Serializers.LONG_SERIALIZER.decode(encode);
        assertEquals(l, decode);
    }

    @Test
    public void testBooleanSerializer() {
        Boolean b = Boolean.FALSE;
        byte[] encode = Serializers.BOOLEAN_SERIALIZER.encode(b);
        assertEquals(1, encode.length);
        Boolean decode = Serializers.BOOLEAN_SERIALIZER.decode(encode);
        assertEquals(b, decode);
    }

    @Test
    public void testCharacterSerializer() {
        Character c = Character.MAX_VALUE;
        byte[] encode = Serializers.CHARACTER_SERIALIZER.encode(c);
        assertEquals(2, encode.length);
        Character decode = Serializers.CHARACTER_SERIALIZER.decode(encode);
        assertEquals(c, decode);
    }

    @Test
    public void testFloatSerializer() {
        Float f = Float.MAX_VALUE;
        byte[] encode = Serializers.FLOAT_SERIALIZER.encode(f);
        assertEquals(4, encode.length);
        Float decode = Serializers.FLOAT_SERIALIZER.decode(encode);
        assertEquals(f, decode);
    }

    @Test
    public void testDoubleSerializer() {
        Double d = Double.MAX_VALUE;
        byte[] encode = Serializers.DOUBLE_SERIALIZER.encode(d);
        assertEquals(8, encode.length);
        Double decode = Serializers.DOUBLE_SERIALIZER.decode(encode);
        assertEquals(d, decode);
    }

    @Test
    public void testStringSerializer() {
        String s = "test string";
        byte[] encode = Serializers.STRING_SERIALIZER.encode(s);
        assertEquals(s.length(), encode.length);
        String decode = Serializers.STRING_SERIALIZER.decode(encode);
        assertEquals(s, decode);
    }

    @Test
    public void testByteArraySerializer() {
        byte[] bytes = new byte[0];
        byte[] encode = Serializers.BYTE_ARRAY_SERIALIZER.encode(bytes);
        assertArrayEquals(bytes, encode);
        byte[] decode = Serializers.BYTE_ARRAY_SERIALIZER.decode(encode);
        assertArrayEquals(bytes, decode);
    }

    @Test
    public void testObjectArraySerializer() {
        Person p = new Person("Jack", 40);
        byte[] encode = Serializers.OBJECT_SERIALIZER.encode(p);
        assertTrue(encode.length > 0);
        Person decode = (Person) Serializers.OBJECT_SERIALIZER.decode(encode);
        assertEquals(p, decode);
    }

    static class Person implements Serializable {
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person person = (Person) o;
            return age == person.age &&
                    Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
    }

}