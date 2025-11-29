package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class QueryCodecs {
    public static final Codec<Api.Query, Query> API_TO_AVRO = QueryApiToAvroCodec.INSTANCE;

    public static final BiCodec<Consumer<OutputStream>, Query, Query, InputStream> AVRO_TO_STREAM
            = QueryAvroToStreamCodec.INSTANCE;

    public static final BiCodec<Consumer<OutputStream>, Api.Query, Api.Query, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codec<Query, byte[]> AVRO_TO_BYTES = QueryAvroToBytesCodec.INSTANCE;

    public static final Codec<Api.Query, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);
}
