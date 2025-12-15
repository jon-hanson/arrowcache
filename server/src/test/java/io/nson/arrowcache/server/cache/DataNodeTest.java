package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.server.AllocatorManager;
import io.nson.arrowcache.server.TestData;
import io.nson.arrowcache.server.utils.TranslateQuery;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
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
import java.util.*;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeTest.class);

    @Test
    public void test() throws IOException {
        final CacheConfig cacheConfig = CacheConfig.loadFromResource("cacheconfig.json");
        try(
                final AllocatorManager allocatorManager = new AllocatorManager(cacheConfig.allocatorMaxSizeConfig());
                final VectorSchemaRoot vsc = TestData.createTestDataVSC(allocatorManager.newChildAllocator("test-data"));
                final DataNode dataNode = new DataNode("abc", "id", allocatorManager, vsc.getSchema());
        ) {
            final VectorUnloader unloader = new VectorUnloader(vsc);

            new HashMap<>();

            logger.info("Loading testdata1.csv");
            final Map<Integer, Map<String, Object>> testDataMap = TestData.loadTestData(vsc, "testdata1.csv");
            dataNode.add(vsc.getSchema(), unloader.getRecordBatch());

            logger.info("Loading testdata2.csv");
            testDataMap.putAll(TestData.loadTestData(vsc, "testdata2.csv"));
            dataNode.add(vsc.getSchema(), unloader.getRecordBatch());

            final Collection<Map<String, Object>> testData = testDataMap.values();

            testQuery(dataNode, testData, TestData.FILTERS1);

            testQuery(dataNode, testData, TestData.FILTERS2);

            testQuery(dataNode, testData, TestData.FILTERS3);

            testQuery(dataNode, testData, TestData.FILTERS4);
        }
    }

    private static void testQuery(
            DataNode dataNode,
            Collection<Map<String, Object>> testData,
            List<Api.Filter<?>>  filters
    ) {
        final Set<Map<String, Object>> results = new HashSet<>();
        final ResultListener resultListener = new ResultListener(results);

        logger.info("Testing filters: {}", filters);
        dataNode.execute(filters, resultListener);

        final Set<Map<String, Object>> matches = matches(testData, filters);

        assertEquals(matches, results);
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
        public void error(Throwable ex) {}

        @Override
        public void completed() {}
    }

    private static final TranslateQuery TRANSLATE_QUERY = new TranslateQuery(false, true);

    private static Set<Map<String, Object>> matches(Collection<Map<String, Object>> testData, List<Api.Filter<?>> filters) {
        filters = TRANSLATE_QUERY.applyFilters(filters);
        Predicate<Map<String, Object>> pred = null;
        for (Api.Filter<?> filter : filters) {
            if (pred == null) {
                pred = filter.alg(FILTER_ALG);
            } else {
                pred = pred.and(filter.alg(FILTER_ALG));
            }
        }

        return testData.stream().filter(pred).collect(toSet());
    }

    private static final Api.Filter.Alg<Predicate<Map<String, Object>>> FILTER_ALG = new Api.Filter.Alg<Predicate<Map<String, Object>>>() {
        @Override
        public Predicate<Map<String, Object>> svFilter(String attribute, Api.SVFilter.Operator op, Object value) {
            return row -> {
                final boolean match = value.equals(row.get(attribute));
                return (op == Api.SVFilter.Operator.EQUALS) == match;
            };
        }

        @Override
        public Predicate<Map<String, Object>> mvFilter(String attribute, Api.MVFilter.Operator op, Set<?> values) {
            return row -> {
                final boolean match = values.contains(row.get(attribute));
                return (op == Api.MVFilter.Operator.IN) == match;
            };
        }
    };
}
