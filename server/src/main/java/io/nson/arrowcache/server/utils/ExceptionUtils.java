package io.nson.arrowcache.server.utils;

import org.slf4j.Logger;

import java.util.function.Function;

public class ExceptionUtils {
    private ExceptionUtils() {}

    public static <E extends Throwable> E logError(
            Logger logger,
            Function<String, E> exceptionCtor,
            String errorMessage
    ) {
        logger.error(errorMessage);
        return exceptionCtor.apply(errorMessage);
    }
}
