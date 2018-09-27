package com.orientechnologies.binary.protocol.binary.serialization;

import com.orientechnologies.binary.abstracts.SerializableInterface;
import com.orientechnologies.binary.protocol.binary.data.Record;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class CSV {

    public static Record deserialize(String input) {
        if (input == null || input.equals("")) {
            return null;
        }
        input = input.trim();

        Record record = new Record();

        byte[] chunk = eatFirstKey(input);
        boolean val = ByteBuffer.wrap(new byte[]{chunk[2]}).getInt() == 1;
        byte key;
        if (val) {
            record.setoClass(new String(new byte[]{chunk[0]}));
            input = new String(new byte[]{chunk[1]});
            chunk = eatKey(input);
            key = chunk[0];
            input = new String(new byte[]{chunk[1]});
        } else {
            key = chunk[0];
            input = new String(new byte[]{chunk[1]});
        }

        if (input.length() == 0) {
            return record;
        }

        chunk = eatValue(input);
        byte value = chunk[0];
        input = new String(new byte[]{chunk[1]});

//        record.setoData(new byte[]{value});

        while (input.length() > 0) {
            if (input.charAt(0) == ',') {
                input = input.substring(1);
            } else {
                break;
            }

            chunk = eatKey(input);
            key = chunk[0];
            input = new String(new byte[]{chunk[1]});
            if (input.length() > 0) {
                chunk = eatValue(input);
                value = chunk[0];
                input = new String(new byte[]{chunk[1]});
//                record.setoData(new byte[]{value});
            } else {
                record.setoData(null);
            }
        }
        return record;
    }

    public static byte[] serialize(Object value) {
        if (value == null) {
            return new byte[]{};
        }
        if (value instanceof String) {
            return value.toString().getBytes();
        } else if (value instanceof SerializableInterface) {
            return serializeDocument((SerializableInterface) value);
        } else {
            return new byte[]{};
        }
    }

    protected static byte[] serializeDocument(SerializableInterface document) {
        Map<String, Object> docs = document.recordSerialize();
        Map<String, Object> elemData = (Map<String, Object>) docs.get("oData");

        byte[][] segments = new byte[elemData.size()][];
        byte[] assembled = new byte[]{};
        int count = 0;
        for (Map.Entry<String, Object> elem: elemData.entrySet()) {
            ByteBuffer wrap = ByteBuffer.allocate(elem.getKey().getBytes().length);
            wrap.put(elem.getKey().getBytes());
            byte[] temp = ArrayUtils.addAll(wrap.array(), ":".getBytes());
            byte[] segment = ArrayUtils.addAll(temp, serialize(elem.getValue()));
            segments[count] = segment;

            if (count < elemData.size() - 1) {
                assembled = ArrayUtils.addAll(assembled, ArrayUtils.addAll(segment, ",".getBytes()));
            } else {
                assembled = ArrayUtils.addAll(assembled, segment);
            }
            count++;
        }

        if (docs.get("oClass") != null) {
            assembled = ArrayUtils.addAll(docs.get("oClass").toString().getBytes(),
                    ArrayUtils.addAll("@".getBytes(), assembled));
        }
        return assembled;
    }

    protected static byte[] eatFirstKey(String input) {
        int length = input.length();
        String collected = "";
        boolean isClassName = false;

        byte[] result = new byte[]{};
        if (input.charAt(0) == '"') {
            result = eatString(input.substring(1));
            return ArrayUtils.addAll(new byte[]{result[0]}, result[1]);
        }

        int i = 0;
        for (;i < length;i++) {
            char c = input.charAt(i);
            if (c == '@') {
                isClassName = true;
                break;
            } else if (c == ':') {
                break;
            } else {
                collected += c;
            }
        }

        ByteBuffer wrap = ByteBuffer.allocate(1);
        wrap.put(isClassName ? (byte) 1: (byte) 0);
        byte[] intermediate = ArrayUtils.addAll(collected.getBytes(), input.substring(i+1).getBytes());
        return ArrayUtils.addAll(intermediate, wrap.array());
    }

    protected static byte[] eatKey(String input) {
        int length = input.length();
        String collected = "";

        byte[] result = new byte[]{};
        if (input.length() >= 1 && input.charAt(0) == '"') {
            result = eatString(input.substring(1));
            return ArrayUtils.addAll(new byte[]{result[0]}, result[1]);
        }

        int i = 0;
        for (;i < length;i++) {
            char c = input.charAt(i);
            if (c == ':') {
                break;
            } else {
                collected += c;
            }
        }
        return ArrayUtils.addAll(collected.getBytes(), input.substring(i+1).getBytes());
    }

    protected static byte[] eatValue(String input) {
        input = StringUtils.stripStart(input, " ");
        char c = input.charAt(0);
        if (c == ',') {
            return ArrayUtils.addAll(new byte[]{0}, input.getBytes());
        } else if (c == '"') {
            return eatString(input.substring(1));
        } else {
            return ArrayUtils.addAll(new byte[]{0}, input.getBytes());
        }
    }

    protected static byte[] eatString(String input) {
        int length = input.length();
        String collected = "";
        int i = 0;
        for (;i < length;i++) {
            char c = input.charAt(i);
            if (c == '\\') {
                i++;
                collected = collected + input.charAt(i);
                continue;
            } else if (c == '"') {
                break;
            } else {
                collected = collected + c;
            }
        }
        return ArrayUtils.addAll(collected.getBytes(), input.substring(i + 1).getBytes());
    }
}