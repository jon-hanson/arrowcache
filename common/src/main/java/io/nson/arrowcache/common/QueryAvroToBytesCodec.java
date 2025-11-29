package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.utils.Codec;

import java.io.*;

public final class QueryAvroToBytesCodec
        implements Codec<Query, byte[]> {
    public static final QueryAvroToBytesCodec INSTANCE = new QueryAvroToBytesCodec();

    @Override
    public byte[] encode(Query query) {
        try {
            return encodeQuery(query);
        } catch (IOException ex) {
            throw new Exception("Encoding error", ex);
        }
    }

    @Override
    public Query decode(byte[] bytes) {
        try {
            return decodeQuery(bytes);
        } catch (IOException ex) {
            throw new Exception("Decoding error", ex);
        }
    }

    public static byte[] encodeQuery(Query query) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        QueryAvroToStreamCodec.encodeQuery(query, baos);
        return baos.toByteArray();
    }

    public static Query decodeQuery(byte[] bytes) throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        return QueryAvroToStreamCodec.decodeQuery(bais);
    }
}
