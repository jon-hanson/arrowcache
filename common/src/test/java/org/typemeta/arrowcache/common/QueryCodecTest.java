package org.typemeta.arrowcache.common;

import org.junit.jupiter.api.Test;
import org.typemeta.arrowcache.common.avro.Query;

import java.io.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class QueryCodecTest {
    private static final BiCodec<Api.Query, Consumer<OutputStream>, Api.Query, InputStream> API_TO_BYTES_CODEC =
            QueryApiToAvroCodec.INSTANCE.andThen(QueryAvroToStreamCodec.INSTANCE);

    @Test
    public void roundTripQueryApiToAvro() {
        final Query avroQuery = QueryApiToAvroCodec.INSTANCE.encode(TestData.API_QUERY);
        assertEquals(TestData.AVRO_QUERY, avroQuery, "API query converted to Avro");

        final Api.Query apiQuery2 = QueryApiToAvroCodec.INSTANCE.decode(avroQuery);
        assertEquals(TestData.API_QUERY, apiQuery2, "API query round-tripped via Avro");
    }

    @Test
    public void roundTripQueryAvroToApi() {
        final Api.Query apiQuery = QueryApiToAvroCodec.INSTANCE.decode(TestData.AVRO_QUERY);
        assertEquals(TestData.API_QUERY, apiQuery, "Avro query converted to API");

        final Query avroQuery2 = QueryApiToAvroCodec.INSTANCE.encode(apiQuery);
        assertEquals(TestData.AVRO_QUERY, avroQuery2, "Avro query round-tripped via API");
    }

    @Test
    public void roundTripQueryAvroToBytes() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        QueryAvroToStreamCodec.INSTANCE.encode(TestData.AVRO_QUERY).accept(baos);

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final Query avroQuery2 = QueryAvroToStreamCodec.INSTANCE.decode(bais);

        assertEquals(TestData.AVRO_QUERY, avroQuery2, "Avro query round-tripped via byte array");
        assertTrue(bais.available() == 0, "Byte array stream has been completely consumed");
    }

    @Test
    public void roundTripQueryApiToBytes() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        API_TO_BYTES_CODEC.encode(TestData.API_QUERY).accept(baos);

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final Api.Query apiQuery2 = API_TO_BYTES_CODEC.decode(bais);

        assertEquals(TestData.API_QUERY, apiQuery2, "API query round-tripped via byte array");
        assertTrue(bais.available() == 0, "Byte array stream has been completely consumed");
    }
}
