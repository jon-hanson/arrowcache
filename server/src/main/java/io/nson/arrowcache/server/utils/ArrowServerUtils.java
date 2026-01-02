package io.nson.arrowcache.server.utils;

import org.apache.arrow.flight.CallStatus;
import org.slf4j.Logger;

public class ArrowServerUtils {
    private ArrowServerUtils() {}

    public static CallStatus logError(CallStatus callStatus, Logger logger, String msg) {
        logger.error(msg);
        return callStatus.withDescription(msg);
    }
}
