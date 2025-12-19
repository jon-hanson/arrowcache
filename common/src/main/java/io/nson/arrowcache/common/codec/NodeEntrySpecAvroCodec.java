package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.avro.NodeEntrySpec;
import io.nson.arrowcache.common.avro.Rows;
import io.nson.arrowcache.common.utils.Codec;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class NodeEntrySpecAvroCodec implements Codec<Api.NodeEntrySpec, NodeEntrySpec> {

    public static final NodeEntrySpecAvroCodec INSTANCE = new NodeEntrySpecAvroCodec();

    @Override
    public NodeEntrySpec encode(Api.NodeEntrySpec raw) {
        return new NodeEntrySpec(
                raw.path(),
                raw.batchRows().entrySet().stream()
                        .map(en -> encode(en.getKey(), en.getValue()))
                        .collect(Collectors.toList())
        );
    }

    private static Rows encode(int batchIndex, Set<Integer> matches) {
        return new Rows(
                batchIndex,
                new ArrayList<>(matches)
        );
    }

    @Override
    public Api.NodeEntrySpec decode(NodeEntrySpec enc) {
        return new Api.NodeEntrySpec(
                enc.getPath(),
                enc.getMatches().stream()
                        .collect(toMap(
                                matches -> matches.getBatchIndex(),
                                matches -> new TreeSet<>(matches.getMatches())
                        ))
        );
    }
}
