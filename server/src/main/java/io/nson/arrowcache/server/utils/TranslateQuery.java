package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.common.Model;
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

    public List<Model.Filter<?>> applyFilters(List<Model.Filter<?>> filters) {
        return filters.stream()
                .map(this::applyFilter)
                .collect(toList());
    }

    public Model.Filter<?> applyFilter(Model.Filter<?> filter) {
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

    private final Model.Filter.Alg<Model.Filter<?>> TRANSLATE_STR_ALG = new Model.Filter.Alg<>() {
        @Override
        public Model.Filter<?> svFilter(String attribute, Model.SVFilter.Operator op, Object value) {
            return new Model.SVFilter<>(attribute, op, applyValue(value));
        }

        @Override
        public Model.Filter<?> mvFilter(String attribute, Model.MVFilter.Operator op, Set<?> values) {
            return new Model.MVFilter<>(attribute, op, applyValues(values));
        }
    };
}
