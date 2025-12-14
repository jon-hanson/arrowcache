package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.Delete;
import io.nson.arrowcache.common.utils.Codec;

import java.util.ArrayList;
import java.util.Optional;
import java.util.TreeSet;

public class DeleteToAvroCodec implements Codec<Api.Delete, Delete> {

    public static final DeleteToAvroCodec INSTANCE = new DeleteToAvroCodec();

    @Override
    public Delete encode(Api.Delete raw) {
        return new Delete(
                new ArrayList<>(raw.paths()),
                raw.query().map(QueryToAvroCodec.INSTANCE::encode).orElse(null)
        );
    }

    @Override
    public Api.Delete decode(Delete enc) {
        return new Api.Delete(
                new TreeSet<>(enc.getPaths()),
                Optional.ofNullable(enc.getQuery()).map(QueryToAvroCodec.INSTANCE::decode)
        );
    }
}
