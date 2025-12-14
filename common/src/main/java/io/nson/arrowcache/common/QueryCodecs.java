package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class QueryCodecs {
    public static final Codec<Api.Query, Query> API_TO_AVRO = QueryToAvroCodec.INSTANCE;

    public static final BiCodec<Consumer<OutputStream>, Query, Query, InputStream> AVRO_TO_STREAM
                = new BiCodec<>() {

        @Override
        public Consumer<OutputStream> encode(Query raw) {
            return os -> {
                try {
                    Query.getEncoder().encode(raw, os);
                } catch (IOException ex) {
                    throw new BiCodec.Exception("Encoding error", ex);
                }
            };
        }

        @Override
        public Query decode(InputStream enc) {
            try {
                return Query.getDecoder().decode(enc);
            } catch (IOException ex) {
                throw new BiCodec.Exception("Decoding error", ex);
            }
        }
    };

    public static final BiCodec<Consumer<OutputStream>, Api.Query, Api.Query, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codec<Query, byte[]> AVRO_TO_BYTES = new Codec<>() {

        @Override
        public byte[] encode(Query raw) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            AVRO_TO_STREAM.encode(raw).accept(baos);
            return baos.toByteArray();
        }

        @Override
        public Query decode(byte[] enc) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(enc);
            return AVRO_TO_STREAM.decode(bais);
        }
    };

    public static final Codec<Api.Query, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);
}
