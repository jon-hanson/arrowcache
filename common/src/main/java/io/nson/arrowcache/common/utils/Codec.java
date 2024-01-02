package io.nson.arrowcache.common.utils;

import java.util.function.Function;

public interface Codec<T1, T2> extends BiCodec<T1, T2, T1, T2> {
    class Exception extends BiCodec.Exception {
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

    static <T1, T2> Codec<T1, T2> valueOf(Function<T1, T2> encoder, Function<T2, T1> decoder) {
        return new Codec<T1, T2>() {
            @Override
            public T2 encode(T1 t1) {
                return encoder.apply(t1);
            }

            @Override
            public T1 decode(T2 t2) {
                return decoder.apply(t2);
            }
        };
    }

    T2 encode(T1 t1);

    T1 decode(T2 t2);

    default <T3> Codec<T1, T3> andThen(Codec<T2, T3> next) {
        return valueOf(
                t1 -> next.encode(encode(t1)),
                t3 -> decode(next.decode(t3))
        );
    }

    default <T0> Codec<T0, T2> combine(Codec<T0, T1> before) {
        return valueOf(
                t0 -> encode(before.encode(t0)),
                t2 -> before.decode(decode(t2))
        );
    }
}
