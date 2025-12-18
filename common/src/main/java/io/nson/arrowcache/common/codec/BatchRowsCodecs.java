package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.NodeEntrySpec;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class BatchRowsCodecs {
    public static final Codec<Api.NodeEntrySpec, NodeEntrySpec> API_TO_AVRO = MatchesToAvroCodec.INSTANCE;

    public static final BiCodec<Consumer<OutputStream>, NodeEntrySpec, NodeEntrySpec, InputStream> AVRO_TO_STREAM
            = new BiCodec<>() {

        @Override
        public Consumer<OutputStream> encode(NodeEntrySpec raw) {
            return os -> {
                try {
                    NodeEntrySpec.getEncoder().encode(raw, os);
                } catch (IOException ex) {
                    throw new RuntimeException("Encoding error", ex);
                }
            };
        }

        @Override
        public NodeEntrySpec decode(InputStream enc) {
            try {
                return NodeEntrySpec.getDecoder().decode(enc);
            } catch (IOException ex) {
                throw new RuntimeException("Decoding error", ex);
            }
        }
    };

    public static final BiCodec<Consumer<OutputStream>, Api.NodeEntrySpec, Api.NodeEntrySpec, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codec<NodeEntrySpec, byte[]> AVRO_TO_BYTES = new Codec<>() {

        @Override
        public byte[] encode(NodeEntrySpec raw) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            AVRO_TO_STREAM.encode(raw).accept(baos);
            return baos.toByteArray();
        }

        @Override
        public NodeEntrySpec decode(byte[] enc) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(enc);
            return AVRO_TO_STREAM.decode(bais);
        }
    };

    public static final Codec<Api.NodeEntrySpec, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);
}
