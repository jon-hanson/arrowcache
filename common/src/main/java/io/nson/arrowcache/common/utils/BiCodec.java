package io.nson.arrowcache.common.utils;

import java.util.function.Function;

/**
 * A BiCodec instance encodes values of type ENC2 to values of type ENC1,
 * and decodes values of type DEC2 to values of type DEC1.
 * In effect, it's a generalisation of Codec.
 */
public interface BiCodec<ENC_R, ENC_T, DEC_R, DEC_T> {
    static <ENC_R, ENC_T, DEC_R, DEC_T> BiCodec<ENC_R, ENC_T, DEC_R, DEC_T> valueOf(
            Function<ENC_T, ENC_R> encoder,
            Function<DEC_T, DEC_R> decoder
    ) {
        return new BiCodec<>() {
            @Override
            public ENC_R encode(ENC_T raw) {
                return encoder.apply(raw);
            }

            @Override
            public DEC_R decode(DEC_T enc) {
                return decoder.apply(enc);
            }
        };
    }

    ENC_R encode(ENC_T raw);

    DEC_R decode(DEC_T enc);

    default <ENC_3, DEC_3> BiCodec<ENC_3, ENC_T, DEC_R, DEC_3> andThen(BiCodec<ENC_3, ENC_R, DEC_T, DEC_3> next) {
        return valueOf(
                enc -> next.encode(encode(enc)),
                dec -> decode(next.decode(dec))
        );
    }
}

