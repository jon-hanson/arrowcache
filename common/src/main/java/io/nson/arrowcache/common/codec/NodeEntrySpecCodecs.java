package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.NodeEntrySpec;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class NodeEntrySpecCodecs {
    private NodeEntrySpecCodecs() {}

    public static final Codec<Api.NodeEntrySpec, NodeEntrySpec> API_TO_AVRO = NodeEntrySpecAvroCodec.INSTANCE;

    public static final Codecs.AvroStreamCodec<NodeEntrySpec> AVRO_TO_STREAM =
            new Codecs.AvroStreamCodec<>(NodeEntrySpec.getEncoder(), NodeEntrySpec.getDecoder()) {};

    public static final BiCodec<Consumer<OutputStream>, Api.NodeEntrySpec, Api.NodeEntrySpec, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codecs.AvroBytesCodec<NodeEntrySpec> AVRO_TO_BYTES = new Codecs.AvroBytesCodec<>(AVRO_TO_STREAM) {};

    public static final Codec<Api.NodeEntrySpec, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);
}
