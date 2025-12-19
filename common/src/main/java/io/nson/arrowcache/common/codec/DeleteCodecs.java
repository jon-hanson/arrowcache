package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.Delete;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class DeleteCodecs {
    private DeleteCodecs() {}

    public static final Codec<Api.Delete, Delete> API_TO_AVRO = DeleteAvroCodec.INSTANCE;

    public static final Codecs.AvroStreamCodec<Delete> AVRO_TO_STREAM =
            new Codecs.AvroStreamCodec<>(Delete.getEncoder(), Delete.getDecoder()) {};

    public static final BiCodec<Consumer<OutputStream>, Api.Delete, Api.Delete, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codecs.AvroBytesCodec<Delete> AVRO_TO_BYTES = new Codecs.AvroBytesCodec<>(AVRO_TO_STREAM) {};

    public static final Codec<Api.Delete, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);

}
