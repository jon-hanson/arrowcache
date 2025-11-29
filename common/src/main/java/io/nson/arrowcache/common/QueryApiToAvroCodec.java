package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.*;
import io.nson.arrowcache.common.utils.*;

import java.util.*;

public final class QueryApiToAvroCodec implements Codec<Api.Query, Query> {
    public static final QueryApiToAvroCodec INSTANCE = new QueryApiToAvroCodec();

    @Override
    public Query encode(Api.Query query) {
        return new Query(
                Functors.listMap(query.filters(), QueryApiToAvroCodec::encode)
        );
    }

    private static <T> Object encode(Api.Filter<T> filter) {
        if (filter instanceof Api.SVFilter) {
            return encode((Api.SVFilter<?>)filter);
        } else if (filter instanceof Api.MVFilter) {
            return encode((Api.MVFilter<?>)filter);
        } else {
            throw new Codec.Exception("Unrecognised Api.Filter type - " + filter.getClass());
        }
    }

    private static <T> SVFilter encode(Api.SVFilter<T> svFilter) {
        return new SVFilter(
                svFilter.attribute(),
                encode(svFilter.operator()),
                svFilter.value()
        );
    }

    private static SVOperator encode(Api.SVFilter.Operator svOperator) {
        switch(svOperator) {
            case EQUALS:
                return SVOperator.EQUALS;
            case NOT_EQUALS:
                return SVOperator.NOT_EQUALS;
            default:
                throw new Codec.Exception("Unrecognised SVFilter.Operator value - " + svOperator);
        }
    }

    private static <T> MVFilter encode(Api.MVFilter<T> mvFilter) {
        return new MVFilter(
                mvFilter.attribute(),
                encode(mvFilter.operator()),
                new ArrayList<>(mvFilter.values())
        );
    }

    private static MVOperator encode(Api.MVFilter.Operator mvOperator) {
        switch(mvOperator) {
            case IN:
                return MVOperator.IN;
            case NOT_IN:
                return MVOperator.NOT_IN;
            default:
                throw new Codec.Exception("Unrecognised MVFilter.Operator value - " + mvOperator);
        }
    }

    @Override
    public Api.Query decode(Query enc) {
        return new Api.Query(
                Functors.listMap(enc.getFilters(), QueryApiToAvroCodec::decodeFilter)
        );
    }

    private static <T> Api.Filter<T> decodeFilter(Object filter) {
        if (filter instanceof SVFilter) {
            return decode((SVFilter)filter);
        } else if (filter instanceof MVFilter) {
            return decode((MVFilter)filter);
        } else {
            throw new Codec.Exception("Unrecognised Filter type - " + filter.getClass());
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
                throw new Codec.Exception("Unrecognised SVOperator value - " + svOperator);
        }
    }

    private static <T> Api.MVFilter<T> decode(MVFilter mvFilter) {
        return new Api.MVFilter<T>(
                mvFilter.getAttribute(),
                decode(mvFilter.getOperator()),
                Functors.setMap((List<T>)mvFilter.getValues(), o -> (T)decodeValue(o))
        );
    }

    private static Api.MVFilter.Operator decode(MVOperator mvOperator) {
        switch(mvOperator) {
            case IN:
                return Api.MVFilter.Operator.IN;
            case NOT_IN:
                return Api.MVFilter.Operator.NOT_IN;
            default:
                throw new Codec.Exception("Unrecognised MVOperator value - " + mvOperator);
        }
    }

    private static Object decodeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof CharSequence) {
            return value.toString();
        } else {
            return value;
        }
    }
}
