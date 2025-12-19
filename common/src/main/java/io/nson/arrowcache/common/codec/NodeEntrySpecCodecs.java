package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.avro.NodeEntrySpec;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class NodeEntrySpecCodecs {
    private NodeEntrySpecCodecs() {}

    public static final Codec<Model.NodeEntrySpec, NodeEntrySpec> MODEL_TO_AVRO = NodeEntrySpecAvroCodec.INSTANCE;

    public static final Codecs.AvroStreamCodec<NodeEntrySpec> AVRO_TO_STREAM =
            new Codecs.AvroStreamCodec<>(NodeEntrySpec.getEncoder(), NodeEntrySpec.getDecoder()) {};

    public static final BiCodec<Consumer<OutputStream>, Model.NodeEntrySpec, Model.NodeEntrySpec, InputStream> MODEL_TO_STREAM =
            MODEL_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codecs.AvroBytesCodec<NodeEntrySpec> AVRO_TO_BYTES = new Codecs.AvroBytesCodec<>(AVRO_TO_STREAM) {};

    public static final Codec<Model.NodeEntrySpec, byte[]> MODEL_TO_BYTES = MODEL_TO_AVRO.andThen(AVRO_TO_BYTES);
}
