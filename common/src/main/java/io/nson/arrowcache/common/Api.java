package io.nson.arrowcache.common;

import java.util.*;

public abstract class Api {
    private Api() {}

    public static abstract class Filter<T> {

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

    public static class SVFilter<T> extends Filter<T> {
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
            } else if (!super.equals(rhs)) {
                return false;
            } else {
                final SVFilter<?> rhsT = (SVFilter<?>) rhs;
                return operator == rhsT.operator && Objects.equals(value, rhsT.value);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), operator, value);
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

    public static class MVFilter<T> extends Filter<T> {
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
            } else if (!super.equals(rhs)) {
                return false;
            } else {
                final MVFilter<?> rhsT = (MVFilter<?>) rhs;
                return operator == rhsT.operator && Objects.equals(values, rhsT.values);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), operator, values);
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

    public static class Query {
        private final List<Filter<?>> filters;

        public Query(List<Filter<?>> filters) {
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
                return Objects.equals(filters, rhsT.filters);
            }
        }

        @Override
        public int hashCode() {
            return filters.hashCode();
        }

        public List<Filter<?>> filters() {
            return filters;
        }

        @Override
        public String toString() {
            return "Query{" +
                    "filters=" + filters +
                    '}';
        }
    }
}
