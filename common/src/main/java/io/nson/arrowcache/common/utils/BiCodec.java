package io.nson.arrowcache.common.utils;

import java.util.function.Function;

public interface BiCodec<F1, F2, R1, R2> {
    class Exception extends RuntimeException {
        public Exception() {
        }

        public Exception(String message) {
            super(message);
        }

        public Exception(String message, Throwable cause) {
            super(message, cause);
        }

        public Exception(Throwable cause) {
            super(cause);
        }

        public Exception(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    static <F1, F2, R1, R2> BiCodec<F1, F2, R1, R2> valueOf(Function<F1, F2> encoder, Function<R2, R1> decoder) {
        return new BiCodec<>() {
            @Override
            public F2 encode(F1 raw) {
                return encoder.apply(raw);
            }

            @Override
            public R1 decode(R2 dec) {
                return decoder.apply(dec);
            }
        };
    }

    F2 encode(F1 raw);

    R1 decode(R2 dec);


    default <F3, R3> BiCodec<F1, F3, R1, R3> andThen(BiCodec<F2, F3, R2, R3> next) {
        return valueOf(
                f1 -> next.encode(encode(f1)),
                r3 -> decode(next.decode(r3))
        );
    }

    default <F0, R0> BiCodec<F0, F2, R0, R2> combine(BiCodec<F0, F1, R0, R1> before) {
        return valueOf(
                f0 -> encode(before.encode(f0)),
                r2 -> before.decode(decode(r2))
        );
    }
}

