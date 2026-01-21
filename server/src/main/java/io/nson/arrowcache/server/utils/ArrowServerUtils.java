package io.nson.arrowcache.server.utils;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;

import java.util.List;

public class ArrowServerUtils {
    private ArrowServerUtils() {}

    public static final Schema EMPTY_SCHEMA = new Schema(List.of(), null);

    public static CallStatus exception(CallStatus callStatus, Logger logger, String msg) {
        logger.error(msg);
        return callStatus.withDescription(msg);
    }
}
