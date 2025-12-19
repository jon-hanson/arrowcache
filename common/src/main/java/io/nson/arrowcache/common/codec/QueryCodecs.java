package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class QueryCodecs {
    private QueryCodecs() {}

    public static final Codec<Api.Query, Query> API_TO_AVRO = QueryAvroCodec.INSTANCE;

    public static final Codecs.AvroStreamCodec<Query> AVRO_TO_STREAM =
            new Codecs.AvroStreamCodec<>(Query.getEncoder(), Query.getDecoder()) {};

    public static final BiCodec<Consumer<OutputStream>, Api.Query, Api.Query, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codecs.AvroBytesCodec<Query> AVRO_TO_BYTES = new Codecs.AvroBytesCodec<>(AVRO_TO_STREAM) {};

    public static final Codec<Api.Query, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);
}
