package org.typemeta.arrowcache.common;

import java.util.*;

public class TestData {
    public static final Api.Query QUERY;

    static {
        final List<Api.Filter> filters = new ArrayList<>();
        filters.add(new Api.SVFilter(
                "name",
                Api.SVFilter.Operator.NOT_EQUALS,
                "test"
        ));
        filters.add(new Api.MVFilter(
                "aliases",
                Api.MVFilter.Operator.IN,
                Set.of("alpha", "beta", "gamma")
        ));
        QUERY = new Api.Query(filters);
    }

}
