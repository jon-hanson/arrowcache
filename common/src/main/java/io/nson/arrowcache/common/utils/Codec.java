package io.nson.arrowcache.common.utils;

import java.util.function.Function;

public interface Codec<RAW, ENC> extends BiCodec<ENC, RAW, RAW, ENC> {
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

    static <RAW, ENC> Codec<RAW, ENC> valueOf(Function<RAW, ENC> encoder, Function<ENC, RAW> decoder) {
        return new Codec<RAW, ENC>() {
            @Override
            public ENC encode(RAW t1) {
                return encoder.apply(t1);
            }

            @Override
            public RAW decode(ENC enc) {
                return decoder.apply(enc);
            }
        };
    }

    ENC encode(RAW t1);

    RAW decode(ENC enc);

    default <ENC2> Codec<RAW, ENC2> andThen(Codec<ENC, ENC2> next) {
        return valueOf(
                t1 -> next.encode(encode(t1)),
                t3 -> decode(next.decode(t3))
        );
    }
}
