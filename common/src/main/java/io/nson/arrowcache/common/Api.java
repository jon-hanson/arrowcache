package io.nson.arrowcache.common;

import java.util.*;

public abstract class Api {
    private Api() {}

    public static abstract class Filter<T> {

        public static <T> SVFilter<T> eq(String attribute, T value) {
            return new SVFilter<T>(attribute, SVFilter.Operator.EQUALS, value);
        }

        public static <T> SVFilter<T> neq(String attribute, T value) {
            return new SVFilter<T>(attribute, SVFilter.Operator.NOT_EQUALS, value);
        }

        public static <T> MVFilter<T> in(String attribute, Set<T> values) {
            return new MVFilter<T>(attribute, MVFilter.Operator.IN, values);
        }

        public static <T> MVFilter<T> notIn(String attribute, Set<T> values) {
            return new MVFilter<T>(attribute, MVFilter.Operator.NOT_IN, values);
        }

        public interface Operator {}

        public interface Alg<U> {
            U svFilter(String attribute, SVFilter.Operator op, Object value);
            U mvFilter(String attribute, MVFilter.Operator op, Set<?> values);
        }

        protected final String attribute;

        public Filter(String attribute) {
            this.attribute = Objects.requireNonNull(attribute);
        }

        @Override
        public boolean equals(Object rhs) {
            if (this == rhs) {
                return true;
            } else if (rhs == null || getClass() != rhs.getClass()) {
                return false;
            } else {
                final Filter<?> rhsT = (Filter<?>) rhs;
                return Objects.equals(attribute, rhsT.attribute);
            }
        }

        @Override
        public int hashCode() {
            return attribute.hashCode();
        }

        @Override
        public String toString() {
            return "Filter{" +
                    "attribute='" + attribute + '\'' +
                    '}';
        }

        public String attribute() {
            return attribute;
        }

        public abstract Operator operator();

        public abstract <U> U alg(Alg<U> alg);
    }

    public static final class SVFilter<T> extends Filter<T> {
        public enum Operator implements Filter.Operator {
            EQUALS, NOT_EQUALS
        }

        private final Operator operator;
        private final T value;

        public SVFilter(String attribute, Operator operator, T value) {
            super(attribute);
            this.operator = Objects.requireNonNull(operator);
            this.value = Objects.requireNonNull(value);
        }

        @Override
        public boolean equals(Object rhs) {
            if (this == rhs) {
                return true;
            } else if (rhs == null || getClass() != rhs.getClass()) {
                return false;
            } else {
                final SVFilter<?> rhsT = (SVFilter<?>) rhs;
                return Objects.equals(attribute, rhsT.attribute) &&
                        operator == rhsT.operator &&
                        Objects.equals(value, rhsT.value);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(attribute, operator, value);
        }

        @Override
        public String toString() {
            return "SVFilter{" +
                    "attribute='" + attribute + '\'' +
                    ", operator=" + operator +
                    ", value='" + value + '\'' +
                    "}";
        }

        public Operator operator() {
            return operator;
        }

        @Override
        public <U> U alg(Alg<U> alg) {
            return alg.svFilter(attribute, operator, value);
        }

        public T value() {
            return value;
        }
    }

    public static final class MVFilter<T> extends Filter<T> {
        public enum Operator implements Filter.Operator {
            IN, NOT_IN
        }

        private final Operator operator;
        private final Set<T> values;

        public MVFilter(String attribute, Operator operator, Set<T> values) {
            super(attribute);
            this.operator = Objects.requireNonNull(operator);
            this.values = Objects.requireNonNull(values);
        }

        @Override
        public boolean equals(Object rhs) {
            if (this == rhs) {
                return true;
            } else if (rhs == null || getClass() != rhs.getClass()) {
                return false;
            } else {
                final MVFilter<?> rhsT = (MVFilter<?>) rhs;
                return Objects.equals(attribute, rhsT.attribute) &&
                        operator == rhsT.operator &&
                        Objects.equals(values, rhsT.values);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(attribute, operator, values);
        }

        @Override
        public String toString() {
            return "MVFilter{" +
                    "attribute='" + attribute + '\'' +
                    ", operator=" + operator +
                    ", value='" + values + '\'' +
                    "}";
        }

        public Operator operator() {
            return operator;
        }

        @Override
        public <U> U alg(Alg<U> alg) {
            return alg.mvFilter(attribute, operator, values);
        }

        public Set<T> values() {
            return values;
        }
    }

    public static final class Query {
        private final String path;
        private final List<Filter<?>> filters;

        public Query(String path, List<Filter<?>> filters) {
            this.path = path;
            this.filters = Objects.requireNonNull(filters);
        }

        @Override
        public boolean equals(Object rhs) {
            if (this == rhs) {
                return true;
            } else if (rhs == null || getClass() != rhs.getClass()) {
                return false;
            } else {
                final Query rhsT = (Query) rhs;
                return Objects.equals(path, rhsT.path) &&
                        Objects.equals(filters, rhsT.filters);
            }
        }

        @Override
        public int hashCode() {
            return filters.hashCode();
        }

        public String path() {
            return path;
        }

        public List<Filter<?>> filters() {
            return filters;
        }

        @Override
        public String toString() {
            return "Query{" +
                    "path=" + path +
                    "filters=" + filters +
                    '}';
        }
    }

    public static final class BatchMatches {
        private final String path;
        private final Map<Integer, Set<Integer>> matches;

        public BatchMatches(String path, Map<Integer, Set<Integer>> matches) {
            this.path = path;
            this.matches = matches;
        }

        public String path() {
            return path;
        }

        public Map<Integer, Set<Integer>> matches() {
            return matches;
        }
    }
}
