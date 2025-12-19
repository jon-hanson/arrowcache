package io.nson.arrowcache.common;

import io.nson.arrowcache.common.avro.Query;
import io.nson.arrowcache.common.codec.QueryCodecs;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CodecsTest {

    @Test
    public void roundTripQueryApiToAvro() {
        final Query avroQuery = QueryCodecs.MODEL_TO_AVRO.encode(TestData.MODEL_QUERY);
        assertEquals(TestData.AVRO_QUERY, avroQuery, "API query converted to Avro");

        final Model.Query apiQuery2 = QueryCodecs.MODEL_TO_AVRO.decode(avroQuery);
        assertEquals(TestData.MODEL_QUERY, apiQuery2, "API query round-tripped via Avro");
    }

    @Test
    public void roundTripQueryAvroToApi() {
        final Model.Query apiQuery = QueryCodecs.MODEL_TO_AVRO.decode(TestData.AVRO_QUERY);
        assertEquals(TestData.MODEL_QUERY, apiQuery, "Avro query converted to API");

        final Query avroQuery2 = QueryCodecs.MODEL_TO_AVRO.encode(apiQuery);
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
        final Query dummy = QueryCodecs.MODEL_TO_AVRO.encode(TestData.MODEL_QUERY);
        final byte[] dummy2 = QueryCodecs.AVRO_TO_BYTES.encode(dummy);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        QueryCodecs.MODEL_TO_STREAM.encode(TestData.MODEL_QUERY).accept(baos);

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final Model.Query apiQuery2 = QueryCodecs.MODEL_TO_STREAM.decode(bais);

        assertEquals(TestData.MODEL_QUERY, apiQuery2, "API query round-tripped via byte array");
        assertTrue(bais.available() == 0, "Byte array stream has been completely consumed");
    }
}
