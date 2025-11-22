package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.utils.BiCodec;

import java.io.*;
import java.util.function.Consumer;

public final class QueryAvroToStreamCodec
        implements BiCodec<Consumer<OutputStream>, Query, Query, InputStream> {
    public static final QueryAvroToStreamCodec INSTANCE = new QueryAvroToStreamCodec();

    @Override
    public Consumer<OutputStream> encode(Query query) {
        return os -> {
            try {
                Query.getEncoder().encode(query, os);
            } catch (IOException ex) {
                throw new BiCodec.Exception("Encoding error", ex);
            }
        };
    }

    @Override
    public Query decode(InputStream enc) {
        try {
            return Query.getDecoder().decode(enc);
        } catch (IOException ex) {
            throw new BiCodec.Exception("Decoding error", ex);
        }
    }

    public static void encodeQuery(Query query, OutputStream os) throws IOException {
        Query.getEncoder().encode(query, os);
    }

    public static Query decodeQuery(InputStream is) throws IOException {
        return Query.getDecoder().decode(is);
    }
}
