package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.BatchMatches;
import io.nson.arrowcache.common.avro.Matches;
import io.nson.arrowcache.common.utils.Codec;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class MatchesToAvroCodec implements Codec<Api.BatchMatches, BatchMatches> {

    public static final MatchesToAvroCodec INSTANCE = new MatchesToAvroCodec();

    @Override
    public BatchMatches encode(Api.BatchMatches raw) {
        return new BatchMatches(
                raw.path(),
                raw.matches().entrySet().stream()
                        .map(en -> encode(en.getKey(), en.getValue()))
                        .collect(Collectors.toList())
        );
    }

    private static Matches encode(int batchIndex, Set<Integer> matches) {
        return new Matches(
                batchIndex,
                new ArrayList<>(matches)
        );
    }

    @Override
    public Api.BatchMatches decode(BatchMatches enc) {
        return new Api.BatchMatches(
                enc.getPath(),
                enc.getMatches().stream()
                        .collect(toMap(
                                matches -> matches.getBatchIndex(),
                                matches -> new TreeSet<>(matches.getMatches())
                        ))
        );
    }
}
