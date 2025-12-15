package io.nson.arrowcache.server.utils;

import org.apache.arrow.flight.CallStatus;
import org.slf4j.Logger;

public class ArrowServerUtils {
    private ArrowServerUtils() {}

    public static RuntimeException internal(Logger logger, String msg, Exception ex) {
        logger.error(msg);
        return CallStatus.INTERNAL
                .withDescription(msg)
                .withCause(ex)
                .toRuntimeException();
    }

    public static RuntimeException invalidArgument(Logger logger, String msg) {
        logger.error(msg);
        return CallStatus.INVALID_ARGUMENT
                .withDescription(msg)
                .toRuntimeException();
    }

    public static RuntimeException notFound(Logger logger, String msg) {
        logger.error(msg);
        return CallStatus.NOT_FOUND
                .withDescription(msg)
                .toRuntimeException();
    }

    public static RuntimeException unknown(Logger logger, String msg, Exception ex) {
        logger.error(msg);
        return CallStatus.UNKNOWN
                .withDescription(msg)
                .withCause(ex)
                .toRuntimeException();
    }
}
