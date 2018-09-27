package com.orientechnologies.binary.protocol.binary.serialization;

import com.orientechnologies.binary.abstracts.SerializableInterface;
import com.orientechnologies.binary.protocol.binary.data.Record;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
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
        Map<String, Object> props = new HashMap<>();

        byte[] key = eatFirstKey(input).getLeft();
        input = input.substring(eatFirstKey(input).getRight());

        byte[] chunk = eatValue(input).getKey();
        input = input.substring(eatValue(input).getRight());
        props.put(new String(key), new String(chunk));

//        record.setoData(new byte[]{value});

        while (input.length() > 0) {
            if (input.charAt(0) == ',') {
                input = input.substring(1);
            } else {
                break;
            }

            key = eatKey(input).getKey();
            input = input.substring(eatKey(input).getRight());
            if (input.length() > 0) {
                chunk = eatValue(input).getKey();
                input = input.substring(eatValue(input).getRight());
                props.put(new String(key), new String(chunk));
//                record.setoData(new byte[]{value});
            } else {
                record.setoData(null);
            }
        }
        record.setoData(props);
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
            byte[] temp = ArrayUtils.addAll(elem.getKey().getBytes(), ":".getBytes());
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

    protected static Triple<byte[], Boolean, Integer> eatFirstKey(String input) {
        int length = input.length();
        String collected = "";
        boolean isClassName = false;

        byte[] result = new byte[]{};
        if (input.charAt(0) == '"') {
            result = eatString(input.substring(1)).getKey();
            return Triple.of(new byte[]{result[0]}, false, -1);
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

        return Triple.of(collected.getBytes(), isClassName, i + 1);
    }

    protected static Pair<byte[], Integer> eatKey(String input) {
        int length = input.length();
        String collected = "";

        byte[] result = new byte[]{};
        if (input.length() >= 1 && input.charAt(0) == '"') {
            result = eatString(input.substring(1)).getKey();
            return Pair.of(result, eatString(input.substring(1)).getRight());
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
        return Pair.of(collected.getBytes(), i+1);
    }

    protected static Pair<byte[], Integer> eatValue(String input) {
        input = StringUtils.stripStart(input, " ");
        char c = input.charAt(0);
        if (c == ',') {
            return Pair.of(new byte[]{0}, 0);
        } else if (c == '"') {
            return eatString(input.substring(1));
        } else {
            return Pair.of(new byte[]{0}, 0);
        }
    }

    protected static Pair<byte[], Integer> eatString(String input) {
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
        return Pair.of(collected.getBytes(), i + 1);
    }
}