package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.BatchMatches;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class MatchesCodecs {
    public static final Codec<Api.BatchMatches, BatchMatches> API_TO_AVRO = MatchesToAvroCodec.INSTANCE;

    public static final BiCodec<Consumer<OutputStream>, BatchMatches, BatchMatches, InputStream> AVRO_TO_STREAM
            = new BiCodec<>() {

        @Override
        public Consumer<OutputStream> encode(BatchMatches raw) {
            return os -> {
                try {
                    BatchMatches.getEncoder().encode(raw, os);
                } catch (IOException ex) {
                    throw new Exception("Encoding error", ex);
                }
            };
        }

        @Override
        public BatchMatches decode(InputStream enc) {
            try {
                return BatchMatches.getDecoder().decode(enc);
            } catch (IOException ex) {
                throw new Exception("Decoding error", ex);
            }
        }
    };

    public static final BiCodec<Consumer<OutputStream>, Api.BatchMatches, Api.BatchMatches, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codec<BatchMatches, byte[]> AVRO_TO_BYTES = new Codec<>() {

        @Override
        public byte[] encode(BatchMatches raw) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            AVRO_TO_STREAM.encode(raw).accept(baos);
            return baos.toByteArray();
        }

        @Override
        public BatchMatches decode(byte[] enc) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(enc);
            return AVRO_TO_STREAM.decode(bais);
        }
    };

    public static final Codec<Api.BatchMatches, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);

}
