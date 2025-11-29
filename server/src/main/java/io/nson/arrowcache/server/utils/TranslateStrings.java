package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.common.Api;
import org.apache.arrow.vector.util.Text;

import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class TranslateStrings {
    private TranslateStrings() {}

    public static Api.Query applyQuery(Api.Query query) {
        return new Api.Query(
                query.filters().stream()
                        .map(TranslateStrings::applyFilter)
                        .collect(toList())
        );
    }

    public static Api.Filter<?> applyFilter(Api.Filter<?> filter) {
        return filter.alg(TRANSLATE_STR_ALG);
    }

    public static Set<Object> applyValues(Set<?> values) {
        return values.stream()
                .map(TranslateStrings::applyValue)
                .collect(toSet());
    }

    public static Object applyValue(Object value) {
        if (value instanceof String) {
            return new Text((String) value);
        } else {
            return value;
        }
    }

    private static final TranslateFilterAlg TRANSLATE_STR_ALG = new  TranslateFilterAlg();

    private static class TranslateFilterAlg implements Api.Filter.Alg<Api.Filter<?>> {
        @Override
        public Api.Filter<?> svFilter(String attribute, Api.SVFilter.Operator op, Object value) {
            return new Api.SVFilter<>(attribute, op, applyValue(value));
        }

        @Override
        public Api.Filter<?> mvFilter(String attribute, Api.MVFilter.Operator op, Set<?> values) {
            return new Api.MVFilter<>(attribute, op, applyValues(values));
        }
    }
}
