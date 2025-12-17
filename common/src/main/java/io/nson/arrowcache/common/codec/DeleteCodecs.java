package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.Delete;
import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;
import java.util.function.Consumer;

public abstract class DeleteCodecs {
    public static final Codec<Api.Delete, Delete> API_TO_AVRO = DeleteToAvroCodec.INSTANCE;

    public static final BiCodec<Consumer<OutputStream>, Delete, Delete, InputStream> AVRO_TO_STREAM
            = new BiCodec<>() {

        @Override
        public Consumer<OutputStream> encode(Delete raw) {
            return os -> {
                try {
                    Delete.getEncoder().encode(raw, os);
                } catch (IOException ex) {
                    throw new RuntimeException("Encoding error", ex);
                }
            };
        }

        @Override
        public Delete decode(InputStream enc) {
            try {
                return Delete.getDecoder().decode(enc);
            } catch (IOException ex) {
                throw new RuntimeException("Decoding error", ex);
            }
        }
    };

    public static final BiCodec<Consumer<OutputStream>, Api.Delete, Api.Delete, InputStream> API_TO_STREAM =
            API_TO_AVRO.andThen(AVRO_TO_STREAM);

    public static final Codec<Delete, byte[]> AVRO_TO_BYTES = new Codec<>() {

        @Override
        public byte[] encode(Delete raw) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            AVRO_TO_STREAM.encode(raw).accept(baos);
            return baos.toByteArray();
        }

        @Override
        public Delete decode(byte[] enc) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(enc);
            return AVRO_TO_STREAM.decode(bais);
        }
    };

    public static final Codec<Api.Delete, byte[]> API_TO_BYTES = API_TO_AVRO.andThen(AVRO_TO_BYTES);

}
