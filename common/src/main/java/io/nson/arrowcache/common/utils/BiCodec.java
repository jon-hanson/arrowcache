package io.nson.arrowcache.common.utils;

import java.util.function.Function;

/**
 * A BiCodec instance encodes values of type ENC2 to values of type ENC1,
 * and decodes values of type DEC2 to values of type DEC1.
 * In effect, it's a generalisation of Codec.
 * @param <ENC1>
 * @param <ENC2>
 * @param <DEC1>
 * @param <DEC2>
 */
public interface BiCodec<ENC1, ENC2, DEC1, DEC2> {
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

    static <ENC1, ENC2, DEC1, DEC2> BiCodec<ENC1, ENC2, DEC1, DEC2> valueOf(
            Function<ENC2, ENC1> encoder,
            Function<DEC2, DEC1> decoder
    ) {
        return new BiCodec<>() {
            @Override
            public ENC1 encode(ENC2 raw) {
                return encoder.apply(raw);
            }

            @Override
            public DEC1 decode(DEC2 enc) {
                return decoder.apply(enc);
            }
        };
    }

    ENC1 encode(ENC2 raw);

    DEC1 decode(DEC2 enc);


    default <ENC3, DEC3> BiCodec<ENC3, ENC2, DEC1, DEC3> andThen(BiCodec<ENC3, ENC1, DEC2, DEC3> next) {
        return valueOf(
                enc2 -> next.encode(encode(enc2)),
                dec3 -> decode(next.decode(dec3))
        );
    }
}

