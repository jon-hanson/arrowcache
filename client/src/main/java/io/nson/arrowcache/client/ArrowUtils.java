package io.nson.arrowcache.client;

import org.apache.arrow.flight.Result;

import java.nio.charset.StandardCharsets;

public abstract class ArrowUtils {
    private ArrowUtils() {}

    public static Result stringToResult(String s) {
        return new Result(s.getBytes(StandardCharsets.UTF_8));
    }

    public static String resultToString(Result res) {
        return new String(res.getBody(), StandardCharsets.UTF_8);
    }
}
