package io.nson.arrowcache.common.utils;

import java.util.function.Function;

public interface Codec<RAW, ENC> extends BiCodec<ENC, RAW, RAW, ENC> {

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
                t1 -> next.encode(encode(t1)),
                t3 -> decode(next.decode(t3))
        );
    }
}
