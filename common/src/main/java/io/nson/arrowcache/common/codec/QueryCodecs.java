package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class QueryCodecs {
    private QueryCodecs() {}

    public static final Codec<Model.Query, Query> MODEL_TO_AVRO = QueryAvroCodec.INSTANCE;

    public static final Codecs.AvroStreamCodec<Query> AVRO_TO_STREAM =
            new Codecs.AvroStreamCodec<>(Query.getEncoder(), Query.getDecoder()) {};

    public static final BiCodec<Consumer<OutputStream>, Model.Query, Model.Query, InputStream> MODEL_TO_STREAM =
            MODEL_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codecs.AvroBytesCodec<Query> AVRO_TO_BYTES = new Codecs.AvroBytesCodec<>(AVRO_TO_STREAM) {};

    public static final Codec<Model.Query, byte[]> MODEL_TO_BYTES = MODEL_TO_AVRO.andThen(AVRO_TO_BYTES);
}
