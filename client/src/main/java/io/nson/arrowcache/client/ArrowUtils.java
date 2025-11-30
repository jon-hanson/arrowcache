package io.nson.arrowcache.client;

import io.nson.arrowcache.common.utils.ByteUtils;
import org.apache.arrow.flight.Result;

public abstract class ArrowUtils {
    private ArrowUtils() {}

    public static Result stringToResult(String s) {
        return new Result(ByteUtils.stringToBytes(s));
    }

    public static String resultToString(Result res) {
        return ByteUtils.bytesToString(res.getBody());
    }
}
