package org.typemeta.arrowcache.common;

import org.typemeta.arrowcache.common.avro.*;

import java.util.*;

public class QueryApiToAvroCodec implements Codec<Api.Query, Query> {
    public static final QueryApiToAvroCodec INSTANCE = new QueryApiToAvroCodec();

    @Override
    public Query encode(Api.Query query) {
        return new Query(
                Functors.listMap(query.filters(), QueryApiToAvroCodec::encode)
        );
    }

    private static Object encode(Api.Filter filter) {
        if (filter instanceof Api.SVFilter) {
            return encode((Api.SVFilter)filter);
        } else if (filter instanceof Api.MVFilter) {
            return encode((Api.MVFilter)filter);
        } else {
            throw new Codec.Exception("Unrecognised Api.Filter type - " + filter.getClass());
        }
    }

    private static SVFilter encode(Api.SVFilter svFilter) {
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

    private static MVFilter encode(Api.MVFilter mvFilter) {
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
    public Api.Query decode(Query query) {
        return new Api.Query(
                Functors.listMap(query.getFilters(), QueryApiToAvroCodec::decodeFilter)
        );
    }

    private static Api.Filter decodeFilter(Object filter) {
        if (filter instanceof SVFilter) {
            return decode((SVFilter)filter);
        } else if (filter instanceof MVFilter) {
            return decode((MVFilter)filter);
        } else {
            throw new Codec.Exception("Unrecognised Filter type - " + filter.getClass());
        }
    }

    private static Api.SVFilter decode(SVFilter svFilter) {
        return new Api.SVFilter(
                svFilter.getAttribute().toString(),
                decode(svFilter.getOperator()),
                svFilter.getValue().toString()
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

    private static Api.MVFilter decode(MVFilter mvFilter) {
        return new Api.MVFilter(
                mvFilter.getAttribute().toString(),
                decode(mvFilter.getOperator()),
                Functors.setMap(mvFilter.getValues(), CharSequence::toString)
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
}
