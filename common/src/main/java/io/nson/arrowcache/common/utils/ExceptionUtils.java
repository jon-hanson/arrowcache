package io.nson.arrowcache.common.utils;

import org.slf4j.Logger;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ExceptionUtils {
    private ExceptionUtils() {}

    public static Builder exception(
            Logger logger,
            String errorMessage
    ) {
        logger.error(errorMessage);
        return new Builder(logger, errorMessage);
    }

    public static class Builder {
        private final Logger logger;
        private final String errorMessage;

        Builder(Logger logger, String errorMessage) {
            this.logger = logger;
            this.errorMessage = errorMessage;
        }

        public RuntimeException create() {
            logger.error(errorMessage);
            return new RuntimeException(errorMessage);
        }

        public <E extends Throwable> E create(Function<String, E> exceptionCtor) {
            return exceptionCtor.apply(errorMessage);
        }

        public WithCause cause(Throwable cause) {
            return new WithCause(cause);
        }

        public class WithCause {
            private final Throwable cause;

            public WithCause(Throwable cause) {
                this.cause = cause;
            }

            public RuntimeException create() {
                logger.error(errorMessage, cause);
                return new RuntimeException(errorMessage, cause);
            }

            public <E extends Throwable> E create(BiFunction<String, Throwable, E> exceptionCtor) {
                return exceptionCtor.apply(errorMessage, cause);
            }
        }
    }
}
