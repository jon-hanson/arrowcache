package io.nson.arrowcache.common.codec;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.common.avro.*;
import io.nson.arrowcache.common.utils.Codec;
import io.nson.arrowcache.common.utils.Functors;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;

public final class QueryAvroCodec implements Codec<Api.Query, Query> {
    public static final QueryAvroCodec INSTANCE = new QueryAvroCodec();

    private static final Api.Filter.Alg<Object> ENCODE_FILTER_ALG = new Api.Filter.Alg<>() {

        @Override
        public Object svFilter(String attribute, Api.SVFilter.Operator op, Object value) {
            return new SVFilter(
                    attribute,
                    encode(op),
                    encodeValue(value)
            );
        }

        @Override
        public Object mvFilter(String attribute, Api.MVFilter.Operator op, Set<?> values) {
            return new MVFilter(
                    attribute,
                    encode(op),
                    Functors.listMap(values, QueryAvroCodec::encodeValue)
            );
        }
    };

    @Override
    public Query encode(Api.Query query) {
        return new Query(
                query.path().parts(),
                Functors.listMap(query.filters(), QueryAvroCodec::encodeFilter)
        );
    }

    public static <T> Object encodeFilter(Api.Filter<T> filter) {
        return filter.alg(ENCODE_FILTER_ALG);
    }

    private static SVOperator encode(Api.SVFilter.Operator svOperator) {
        switch(svOperator) {
            case EQUALS:
                return SVOperator.EQUALS;
            case NOT_EQUALS:
                return SVOperator.NOT_EQUALS;
            default:
                throw new RuntimeException("Unrecognised SVFilter.Operator value - " + svOperator);
        }
    }

    private static MVOperator encode(Api.MVFilter.Operator mvOperator) {
        switch(mvOperator) {
            case IN:
                return MVOperator.IN;
            case NOT_IN:
                return MVOperator.NOT_IN;
            default:
                throw new RuntimeException("Unrecognised MVFilter.Operator value - " + mvOperator);
        }
    }

    private static Object encodeValue(Object value) {
        if (value instanceof LocalDate) {
            final LocalDate localDate = (LocalDate) value;
            return new AvroDate(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
        } else {
            return value;
        }
    }

    @Override
    public Api.Query decode(Query enc) {
        return new Api.Query(
                CachePath.valueOf(enc.getPath()),
                Functors.listMap(enc.getFilters(), QueryAvroCodec::decodeFilter)
        );
    }

    public static <T> Api.Filter<T> decodeFilter(Object filter) {
        if (filter instanceof SVFilter) {
            return decode((SVFilter)filter);
        } else if (filter instanceof MVFilter) {
            return decode((MVFilter)filter);
        } else {
            throw new RuntimeException("Unrecognised Filter type - " + filter.getClass());
        }
    }

    private static <T> Api.SVFilter<T> decode(SVFilter svFilter) {
        return new Api.SVFilter<T>(
                svFilter.getAttribute(),
                decode(svFilter.getOperator()),
                (T)decodeValue(svFilter.getValue())
        );
    }

    private static Api.SVFilter.Operator decode(SVOperator svOperator) {
        switch(svOperator) {
            case EQUALS:
                return Api.SVFilter.Operator.EQUALS;
            case NOT_EQUALS:
                return Api.SVFilter.Operator.NOT_EQUALS;
            default:
                throw new RuntimeException("Unrecognised SVOperator value - " + svOperator);
        }
    }

    private static <T> Api.MVFilter<T> decode(MVFilter mvFilter) {
        return new Api.MVFilter<T>(
                mvFilter.getAttribute(),
                decode(mvFilter.getOperator()),
                Functors.setMap((List<T>)mvFilter.getValues(), t -> (T)decodeValue(t))
        );
    }

    private static Api.MVFilter.Operator decode(MVOperator mvOperator) {
        switch(mvOperator) {
            case IN:
                return Api.MVFilter.Operator.IN;
            case NOT_IN:
                return Api.MVFilter.Operator.NOT_IN;
            default:
                throw new RuntimeException("Unrecognised MVOperator value - " + mvOperator);
        }
    }

    private static Object decodeValue(Object value) {
        if (value instanceof CharSequence) {
            return value.toString();
        } else if (value instanceof AvroDate) {
            final AvroDate avroDate = (AvroDate)value;
            return LocalDate.of(avroDate.getYear(), avroDate.getMonth(), avroDate.getDay());
        } else {
            return value;
        }
    }
}
