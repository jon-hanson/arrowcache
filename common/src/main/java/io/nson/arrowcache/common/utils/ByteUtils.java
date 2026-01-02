package io.nson.arrowcache.common.utils;

import java.nio.charset.StandardCharsets;

public abstract class ByteUtils {
    private ByteUtils() {
    }

    public static byte[] stringToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
