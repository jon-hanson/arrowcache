package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.*;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class TestData {

    private static final String ATTR_A = "name";
    private static final String ATTR_B = "aliases";

    private static final String VALUE_1 = "test";
    private static final String VALUE_2 = "alpha";
    private static final String VALUE_3 = "beta";
    private static final String VALUE_4 = "gamma";

    public static final Model.Query MODEL_QUERY =
            new Model.Query(
                    TablePath.valueOf("abc", "def"),
                    List.of(
                            new Model.SVFilter<>(
                                    ATTR_A,
                                    Model.SVFilter.Operator.NOT_EQUALS,
                                    VALUE_1
                            ), new Model.MVFilter<>(
                                    ATTR_B,
                                    Model.MVFilter.Operator.IN,
                                    new TreeSet<>(Set.of(VALUE_2, VALUE_3, VALUE_4))
                            )
                    )
            );

    public static final Query AVRO_QUERY;

    static {
        AVRO_QUERY = Query.newBuilder()
                .setPath(List.of("abc", "def"))
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
