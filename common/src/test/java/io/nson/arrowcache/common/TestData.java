package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.*;

import java.util.*;

public class TestData {
    public static final Api.Query API_QUERY;

    private static final String ATTR_A = "name";
    private static final String ATTR_B = "aliases";

    private static final String VALUE_1 = "test";
    private static final String VALUE_2 = "alpha";
    private static final String VALUE_3 = "beta";
    private static final String VALUE_4 = "gamma";

    static {
        final List<Api.Filter<?>> filters = new ArrayList<>();

        filters.add(new Api.SVFilter<>(
                ATTR_A,
                Api.SVFilter.Operator.NOT_EQUALS,
                VALUE_1
        ));

        filters.add(new Api.MVFilter<>(
                ATTR_B,
                Api.MVFilter.Operator.IN,
                new TreeSet<>(Set.of(VALUE_2, VALUE_3, VALUE_4))
        ));

        API_QUERY = new Api.Query("/abc/d/ef", filters);
    }

    public static final Query AVRO_QUERY;

    static {
        AVRO_QUERY = Query.newBuilder()
                .setFilters(
                        List.of(
                                SVFilter.newBuilder()
                                        .setAttribute(ATTR_A)
                                        .setOperator(SVOperator.NOT_EQUALS)
                                        .setValue(VALUE_1)
                                        .build(),
                                MVFilter.newBuilder()
                                        .setAttribute(ATTR_B)
                                        .setOperator(MVOperator.IN)
                                        .setValues(List.of(VALUE_2, VALUE_3, VALUE_4))
                                        .build()
                        )
                ).build();
    }
}
