package io.nson.arrowcache.common;

import io.nson.arrowcache.common.codec.QueryCodecs;
import org.junit.jupiter.api.Test;
import io.nson.arrowcache.common.avro.Query;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

public class CodecsTest {

    @Test
    public void roundTripQueryApiToAvro() {
        final Query avroQuery = QueryCodecs.API_TO_AVRO.encode(TestData.API_QUERY);
        assertEquals(TestData.AVRO_QUERY, avroQuery, "API query converted to Avro");

        final Api.Query apiQuery2 = QueryCodecs.API_TO_AVRO.decode(avroQuery);
        assertEquals(TestData.API_QUERY, apiQuery2, "API query round-tripped via Avro");
    }

    @Test
    public void roundTripQueryAvroToApi() {
        final Api.Query apiQuery = QueryCodecs.API_TO_AVRO.decode(TestData.AVRO_QUERY);
        assertEquals(TestData.API_QUERY, apiQuery, "Avro query converted to API");

        final Query avroQuery2 = QueryCodecs.API_TO_AVRO.encode(apiQuery);
        assertEquals(TestData.AVRO_QUERY, avroQuery2, "Avro query round-tripped via API");
    }

    @Test
    public void roundTripQueryAvroToBytes() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        QueryCodecs.AVRO_TO_STREAM.encode(TestData.AVRO_QUERY).accept(baos);

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final Query avroQuery2 = QueryCodecs.AVRO_TO_STREAM.decode(bais);

        assertEquals(TestData.AVRO_QUERY, avroQuery2, "Avro query round-tripped via byte array");
        assertTrue(bais.available() == 0, "Byte array stream has been completely consumed");
    }

    @Test
    public void roundTripQueryApiToBytes() {
        final Query dummy = QueryCodecs.API_TO_AVRO.encode(TestData.API_QUERY);
        final byte[] dummy2 = QueryCodecs.AVRO_TO_BYTES.encode(dummy);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        QueryCodecs.API_TO_STREAM.encode(TestData.API_QUERY).accept(baos);

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final Api.Query apiQuery2 = QueryCodecs.API_TO_STREAM.decode(bais);

        assertEquals(TestData.API_QUERY, apiQuery2, "API query round-tripped via byte array");
        assertTrue(bais.available() == 0, "Byte array stream has been completely consumed");
    }
}
