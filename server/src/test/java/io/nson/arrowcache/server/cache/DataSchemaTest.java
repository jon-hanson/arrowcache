package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.RootSchemaConfig;
import io.nson.arrowcache.server.TestData;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataSchemaTest {
    private static final Logger logger = LoggerFactory.getLogger(DataSchemaTest.class);

    @Test
    public void test() throws IOException {
        final RootSchemaConfig schemaConfig = FileUtils.loadFromResource("schemaconfig-test.json", RootSchemaConfig.CODEC);
        try(
                final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                final VectorSchemaRoot vsc = TestData.createTestDataVSC(allocator);
                final DataSchema dataSchema = new DataSchema(allocator, "test", schemaConfig)
        ) {
            final VectorUnloader unloader = new VectorUnloader(vsc);

            logger.info("Loading testdata1.csv");
            final Map<Integer, Map<String, Object>> testDataMap = TestData.loadTestData(vsc, "testdata1.csv");
            final DataTable dataTable1 = dataSchema.getDataTable("abc").orElseThrow();
            dataTable1.addBatch(vsc.getSchema(), unloader.getRecordBatch());

            logger.info("Loading testdata2.csv");
            testDataMap.putAll(TestData.loadTestData(vsc, "testdata2.csv"));
            //final DataTable dataTable2 = dataSchema.getTableOpt("abc").orElseThrow();
            dataTable1.addBatch(vsc.getSchema(), unloader.getRecordBatch());

            final Collection<Map<String, Object>> testData = testDataMap.values();

            testQuery(dataTable1, testData, "id", TestData.KEYS1);

            testQuery(dataTable1, testData, "id", TestData.KEYS2);

            testQuery(dataTable1, testData, "id", TestData.KEYS3);

            testQuery(dataTable1, testData, "id", TestData.KEYS4);
        }
    }

    private static void testQuery(
            DataTable dataTable,
            Collection<Map<String, Object>> testData,
            String keyColumn,
            Set<?> keys
    ) {
        logger.info("Testing filters: {}", keys);

        final Set<Map<String, Object>> actualResults = new HashSet<>();
        dataTable.get(keys, new ResultListener(actualResults));

        final Set<Map<String, Object>> expectedResults = matches(testData, keyColumn, keys);

        assertEquals(expectedResults, actualResults);
    }

    private static Set<Map<String, Object>> matches(
            Collection<Map<String, Object>> testData,
            String keyColumn,
            Set<?> keys
    ) {
        return testData.stream()
                .filter(row -> keys.contains(row.get(keyColumn)))
                .collect(toSet());
    }

    private static class ResultListener implements FlightProducer.ServerStreamListener {

        private final Set<Map<String, Object>> results;

        private VectorSchemaRoot vsc;

        private ResultListener(Set<Map<String, Object>>results) {
            this.results = results;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
            this.vsc = root;
        }

        @Override
        public void putNext() {
            ArrowUtils.toLines(logger::info, vsc);

            final Map<String, Object> row = new HashMap<>();
            for (int r = 0; r < vsc.getRowCount(); r++) {
                for (FieldVector fv : vsc.getFieldVectors()) {
                    Object value = fv.getObject(r);
                    if (value instanceof Text) {
                        value = value.toString();
                    }
                    row.put(fv.getField().getName(), value);
                }
                results.add(row);
            }
        }

        @Override
        public void putNext(ArrowBuf metadata) {}

        @Override
        public void putMetadata(ArrowBuf metadata) {}

        @Override
        public void error(Throwable ex) {
            throw new RuntimeException("Error", ex);
        }

        @Override
        public void completed() {}
    }

}
