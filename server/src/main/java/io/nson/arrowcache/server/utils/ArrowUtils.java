package io.nson.arrowcache.server.utils;

import org.apache.arrow.flight.Result;

import java.nio.charset.StandardCharsets;

public abstract class ArrowUtils {
    private ArrowUtils() {}

    public static Result stringToResult(String s) {
        return new Result(s.getBytes(StandardCharsets.UTF_8));
    }

    public static String resultToString(Result res) {
        return bytesToString(res.getBody());
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
