package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.avro.Delete;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class DeleteCodecs {
    private DeleteCodecs() {}

    public static final Codec<Model.Delete, Delete> MODEL_TO_AVRO = DeleteAvroCodec.INSTANCE;

    public static final Codecs.AvroStreamCodec<Delete> AVRO_TO_STREAM =
            new Codecs.AvroStreamCodec<>(Delete.getEncoder(), Delete.getDecoder()) {};

    public static final BiCodec<Consumer<OutputStream>, Model.Delete, Model.Delete, InputStream> MODEL_TO_STREAM =
            MODEL_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codecs.AvroBytesCodec<Delete> AVRO_TO_BYTES = new Codecs.AvroBytesCodec<>(AVRO_TO_STREAM) {};

    public static final Codec<Model.Delete, byte[]> MODEL_TO_BYTES = MODEL_TO_AVRO.andThen(AVRO_TO_BYTES);

}
