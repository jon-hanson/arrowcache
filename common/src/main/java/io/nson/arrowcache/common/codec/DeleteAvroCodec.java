package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.common.avro.Delete;
import io.nson.arrowcache.common.utils.Codec;

import static java.util.stream.Collectors.toUnmodifiableList;

public class DeleteAvroCodec implements Codec<Api.Delete, Delete> {

    public static final DeleteAvroCodec INSTANCE = new DeleteAvroCodec();

    @Override
    public Delete encode(Api.Delete raw) {
        return new Delete(
                raw.path().toString(),
                raw.filters().stream().map(QueryAvroCodec::encodeFilter).collect(toUnmodifiableList())
        );
    }

    @Override
    public Api.Delete decode(Delete enc) {
        return new Api.Delete(
                CachePath.valueOfConcat(enc.getPath()),
                enc.getFilters().stream().map(QueryAvroCodec::decodeFilter).collect(toUnmodifiableList())
        );
    }
}
