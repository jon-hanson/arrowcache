package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.BatchMatches;
import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class QueryCodecs {
    public static final Codec<Api.Query, Query> QUERY_API_TO_AVRO = QueryToAvroCodec.INSTANCE;

    public static final BiCodec<Consumer<OutputStream>, Query, Query, InputStream> QUERY_AVRO_TO_STREAM
                = new BiCodec<>() {

        @Override
        public Consumer<OutputStream> encode(Query query) {
            return os -> {
                try {
                    Query.getEncoder().encode(query, os);
                } catch (IOException ex) {
                    throw new BiCodec.Exception("Encoding error", ex);
                }
            };
        }

        @Override
        public Query decode(InputStream is) {
            try {
                return Query.getDecoder().decode(is);
            } catch (IOException ex) {
                throw new BiCodec.Exception("Decoding error", ex);
            }
        }
    };

    public static final BiCodec<Consumer<OutputStream>, Api.Query, Api.Query, InputStream> QUERY_API_TO_STREAM =
            QUERY_API_TO_AVRO.andThen(QUERY_AVRO_TO_STREAM);

    public static final Codec<Query, byte[]> QUERY_AVRO_TO_BYTES = new Codec<>() {

        @Override
        public byte[] encode(Query query) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            QUERY_AVRO_TO_STREAM.encode(query).accept(baos);
            return baos.toByteArray();
        }

        @Override
        public Query decode(byte[] bytes) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            return QUERY_AVRO_TO_STREAM.decode(bais);
        }
    };

    public static final Codec<Api.Query, byte[]> QUERY_API_TO_BYTES = QUERY_API_TO_AVRO.andThen(QUERY_AVRO_TO_BYTES);

    public static final Codec<Api.BatchMatches, BatchMatches> MATCHES_API_TO_AVRO = MatchesToAvroCodec.INSTANCE;


    public static final BiCodec<Consumer<OutputStream>, BatchMatches, BatchMatches, InputStream> MATCHES_AVRO_TO_STREAM
            = new BiCodec<>() {

        @Override
        public Consumer<OutputStream> encode(BatchMatches matches) {
            return os -> {
                try {
                    BatchMatches.getEncoder().encode(matches, os);
                } catch (IOException ex) {
                    throw new BiCodec.Exception("Encoding error", ex);
                }
            };
        }

        @Override
        public BatchMatches decode(InputStream is) {
            try {
                return BatchMatches.getDecoder().decode(is);
            } catch (IOException ex) {
                throw new BiCodec.Exception("Decoding error", ex);
            }
        }
    };

    public static final BiCodec<Consumer<OutputStream>, Api.BatchMatches, Api.BatchMatches, InputStream> MATCHES_API_TO_STREAM =
            MATCHES_API_TO_AVRO.andThen(MATCHES_AVRO_TO_STREAM);

    public static final Codec<BatchMatches, byte[]> MATCHES_AVRO_TO_BYTES = new Codec<>() {

        @Override
        public byte[] encode(BatchMatches matches) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            MATCHES_AVRO_TO_STREAM.encode(matches).accept(baos);
            return baos.toByteArray();
        }

        @Override
        public BatchMatches decode(byte[] bytes) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            return MATCHES_AVRO_TO_STREAM.decode(bais);
        }
    };

    public static final Codec<Api.BatchMatches, byte[]> MATCHES_API_TO_BYTES = MATCHES_API_TO_AVRO.andThen(MATCHES_AVRO_TO_BYTES);

}
