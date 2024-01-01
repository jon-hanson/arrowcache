package org.typemeta.arrowcache.common;

import org.junit.jupiter.api.Test;
import org.typemeta.arrowcache.common.avro.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryCodecTest {
    @Test
    public void roundTripQueryApiToAvroCodec() {
        final Query avroQuery = QueryApiToAvroCodec.INSTANCE.encode(TestData.QUERY);
        final Api.Query query2 = QueryApiToAvroCodec.INSTANCE.decode(avroQuery);

        assertEquals(TestData.QUERY, query2, "Round-tripped query");
    }
}
