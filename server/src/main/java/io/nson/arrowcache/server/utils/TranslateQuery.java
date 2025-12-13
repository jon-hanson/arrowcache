package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.common.Api;
import org.apache.arrow.vector.util.Text;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class TranslateQuery {
    private final boolean translateString;
    private final boolean translateDate;

    public TranslateQuery(boolean translateString, boolean translateDate) {
        this.translateString = translateString;
        this.translateDate = translateDate;
    }

    public List<Api.Filter<?>> applyFilters(List<Api.Filter<?>> filters) {
        return filters.stream()
                .map(this::applyFilter)
                .collect(toList());
    }

    public Api.Filter<?> applyFilter(Api.Filter<?> filter) {
        return filter.alg(TRANSLATE_STR_ALG);
    }

    public Set<Object> applyValues(Set<?> values) {
        return values.stream()
                .map(this::applyValue)
                .collect(toSet());
    }

    public Object applyValue(Object value) {
        if (translateString && value instanceof String) {
            return new Text((String) value);
        } else if (translateDate && value instanceof LocalDate) {
            return (int)((LocalDate) value).toEpochDay();
        } else {
            return value;
        }
    }

    private final Api.Filter.Alg<Api.Filter<?>> TRANSLATE_STR_ALG = new Api.Filter.Alg<>() {
        @Override
        public Api.Filter<?> svFilter(String attribute, Api.SVFilter.Operator op, Object value) {
            return new Api.SVFilter<>(attribute, op, applyValue(value));
        }

        @Override
        public Api.Filter<?> mvFilter(String attribute, Api.MVFilter.Operator op, Set<?> values) {
            return new Api.MVFilter<>(attribute, op, applyValues(values));
        }
    };
}
