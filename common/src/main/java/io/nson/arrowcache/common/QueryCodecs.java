package io.nson.arrowcache.common;

import io.nson.arrowcache.common.utils.BiCodec;

import java.io.*;
import java.util.function.Consumer;

public abstract class QueryCodecs {
    public static final QueryApiToAvroCodec API_TO_AVRO = QueryApiToAvroCodec.INSTANCE;

    public static final QueryAvroToStreamCodec AVRO_TO_STREAM = QueryAvroToStreamCodec.INSTANCE;

    public static final BiCodec<Api.Query, Consumer<OutputStream>, Api.Query, InputStream> API_TO_BYTES =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);
}
