package io.nson.arrowcache.server;

import io.nson.arrowcache.common.Api;
import org.apache.arrow.vector.FieldVector;

import java.util.*;

public class QueryLogic {

    private static final class Values<T> {
        Set<T> inclusions = null;
        Set<T> exclusions = null;

        private Set<T> inclusions() {
            if (inclusions == null) {
                inclusions = new HashSet<>();
            }
            return inclusions;
        }

        private Set<T> exclusions() {
            if (exclusions == null) {
                exclusions = new HashSet<>();
            }
            return exclusions;
        }

        void addInclusion(Object value) {
            inclusions().add((T)value);
        }

        void addInclusions(Collection<?> values) {
            inclusions().addAll((Collection)values);
        }

        void addExclusion(Object value) {
            exclusions().add((T)value);
        }

        void addExclusions(Collection<?> values) {
            exclusions().addAll((Collection)values);
        }
    }

    public static class Filter<T> {
        public enum Operator {
            IN, NOT_IN
        }

        private final String attribute;
        private final Operator operator;
        private final Set<T> values;

        private Filter(String attribute, Operator operator, Set<T> values) {
            this.attribute = attribute;
            this.operator = operator;
            this.values = values;
        }

        public String attribute() {
            return attribute;
        }

        public Operator operator() {
            return operator;
        }

        public Set<T> values() {
            return values;
        }

        public boolean match(FieldVector fv, int rowIndex) {
            return (operator == Operator.IN) == values.contains(fv.getObject(rowIndex));
        }
    }

    private final List<Filter<?>> inFilters = new ArrayList<>();
    private final List<Filter<?>> notInFilters = new ArrayList<>();

    public QueryLogic(String keyAttrName, Api.Query query) {

        final Map<String, Values<?>> mapValues = new HashMap<>();

        for (Api.Filter<?> filter : query.filters()) {
            final Values<?> values = mapValues.computeIfAbsent(filter.attribute(), k -> new Values<>());
            if (filter instanceof Api.SVFilter) {
                final Api.SVFilter<?> svFilter = (Api.SVFilter)filter;
                switch (svFilter.operator()) {
                    case EQUALS:
                        values.addInclusion(svFilter.value());
                        break;
                    case NOT_EQUALS:
                        values.addExclusion(svFilter.value());
                        break;
                    default:
                        throw new IllegalStateException("Unknown filter operator: " + svFilter.operator());
                }
            } else if (filter instanceof Api.MVFilter) {
                final Api.MVFilter<?> mvFilter = (Api.MVFilter)filter;
                switch (mvFilter.operator()) {
                    case IN:
                        values.addInclusions(mvFilter.values());
                        break;
                    case NOT_IN:
                        values.addExclusions(mvFilter.values());
                        break;
                    default:
                        throw new IllegalStateException("Unknown filter operator: " + mvFilter.operator());
                }
            }
        }

        if (mapValues.containsKey(keyAttrName)) {
            final Values<?> values = mapValues.get(keyAttrName);
            if (values.inclusions != null) {
                final Set<?> inclusions = values.inclusions;
                if (values.exclusions != null) {
                    inclusions.removeAll(values.exclusions);
                }
                inFilters.add(new Filter<>(keyAttrName, Filter.Operator.IN, inclusions));
            } else if (values.exclusions != null) {
                notInFilters.add(new Filter<>(keyAttrName, Filter.Operator.NOT_IN, values.exclusions));
            }

            mapValues.remove(keyAttrName);
        }

        mapValues.forEach((name, values) -> {
            if (values.inclusions != null) {
                final Set<?> inclusions = values.inclusions;
                if (values.exclusions != null) {
                    inclusions.removeAll(values.exclusions);
                }
                inFilters.add(new Filter<>(name, Filter.Operator.IN, inclusions));
            } else if (values.exclusions != null) {
                notInFilters.add(new Filter<>(name, Filter.Operator.NOT_IN, values.exclusions));
            }
        });
    }

    public int filterCount() {
        return inFilters.size() + notInFilters.size();
    }

    public Filter<?> filter(int filterIndex) {
        if (filterIndex < inFilters.size()) {
            return inFilters.get(filterIndex);
        } else {
            return notInFilters.get(filterIndex - inFilters.size());
        }
    }

    public List<Filter<?>> filters() {
        final List<Filter<?>> filters = new ArrayList<>(inFilters);
        filters.addAll(notInFilters);
        return filters;
    }
}
