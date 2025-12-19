package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.utils.BiCodec;
import io.nson.arrowcache.common.utils.Codec;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;

import java.io.*;
import java.util.function.Consumer;

public final class Codecs {
    private Codecs() {}

    public static abstract class AvroStreamCodec<T>
            implements BiCodec<Consumer<OutputStream>, T, T, InputStream> {

        private final BinaryMessageEncoder<T> encoder;
        private final BinaryMessageDecoder<T> decoder;

        protected AvroStreamCodec(BinaryMessageEncoder<T> encoder, BinaryMessageDecoder<T> decoder) {
            this.encoder = encoder;
            this.decoder = decoder;
        }

        @Override
        public Consumer<OutputStream> encode(T raw) {
            return os -> {
                try {
                    encoder.encode(raw, os);
                } catch (IOException ex) {
                    throw new RuntimeException("Encoding error", ex);
                }
            };
        }

        @Override
        public T decode(InputStream enc) {
            try {
                return decoder.decode(enc);
            } catch (IOException ex) {
                throw new RuntimeException("Decoding error", ex);
            }
        }
    }

    public static abstract class AvroBytesCodec<T> implements Codec<T, byte[]> {

        private final BiCodec<Consumer<OutputStream>, T, T, InputStream> avroToStreamCodec;

        protected AvroBytesCodec(BiCodec<Consumer<OutputStream>, T, T, InputStream> avroToStreamCodec) {
            this.avroToStreamCodec = avroToStreamCodec;
        }

        @Override
        public byte[] encode(T raw) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            avroToStreamCodec.encode(raw).accept(baos);
            return baos.toByteArray();
        }

        @Override
        public T decode(byte[] enc) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(enc);
            return avroToStreamCodec.decode(bais);
        }
    }
}
