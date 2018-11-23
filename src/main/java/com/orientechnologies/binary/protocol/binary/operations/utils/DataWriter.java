package com.orientechnologies.binary.protocol.binary.operations.utils;

import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;

public class DataWriter {

    public static byte[] packInt(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        return buffer.array();
    }

    public static byte[] packString(String value) {
        byte[] valueLength = packInt(value.length());

        return ArrayUtils.addAll(valueLength, value.getBytes());
    }
}