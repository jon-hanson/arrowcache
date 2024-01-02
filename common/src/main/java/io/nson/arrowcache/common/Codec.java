package io.nson.arrowcache.common;

import java.util.function.Function;

public interface Codec<RAW, ENC> extends BiCodec<RAW, ENC, RAW, ENC>{
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
            public ENC encode(RAW raw) {
                return encoder.apply(raw);
            }

            @Override
            public RAW decode(ENC enc) {
                return decoder.apply(enc);
            }
        };
    }

    ENC encode(RAW raw);

    RAW decode(ENC enc);

    default <ENC2> Codec<RAW, ENC2> andThen(Codec<ENC, ENC2> next) {
        return valueOf(
                raw -> next.encode(encode(raw)),
                enc2 -> decode(next.decode(enc2))
        );
    }

    default <RAW2> Codec<RAW2, ENC> combine(Codec<RAW2, RAW> before) {
        return valueOf(
                raw2 -> encode(before.encode(raw2)),
                enc -> before.decode(decode(enc))
        );
    }
}
