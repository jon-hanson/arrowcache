package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.common.utils.ByteUtils;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;

public abstract class ArrowUtils {
    private ArrowUtils() {}

    public static Result stringToResult(String s) {
        return new Result(s.getBytes(StandardCharsets.UTF_8));
    }

    public static String resultToString(Result res) {
        return ByteUtils.bytesToString(res.getBody());
    }

    public static String toString(FlightProducer.CallContext context) {
        return "Context{peerIdentity=" + context.peerIdentity() +
                "; isCancelled=" + context.isCancelled()
                +"}";
    }

    public static String toString(Action action) {
        return "Action{type=" + action.getType() +
                "; body=" + ByteUtils.bytesToString(action.getBody())
                +"}";
    }

    public static String textToString(Text text) {
        return text.toString();
    }
}
