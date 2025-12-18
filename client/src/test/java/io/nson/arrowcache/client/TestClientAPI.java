package io.nson.arrowcache.client;

import io.nson.arrowcache.client.impl.ArrowFlightClientImpl;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.common.utils.FileUtils;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestClientAPI {
    private static final Logger logger = LoggerFactory.getLogger(TestClientAPI.class);

    /*
     * --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
     */
    public static void main(String[] args) throws IOException {

        final ClientConfig clientConfig = FileUtils.loadFromResource("clientconfig.json", ClientConfig.CODEC);
        final CachePath cachePath1 = CachePath.valueOf("abc", "def");
        final CachePath cachePath2 = CachePath.valueOf("abc", "ghi");

        final Location location = Location.forGrpcInsecure(clientConfig.serverHost(), clientConfig.serverPort());

        try (
                final RootAllocator allocator = new RootAllocator();
                final ClientAPI clientAPI = ArrowFlightClientImpl.create(location);
                final VectorSchemaRoot vsc = TestData.createTestDataVSC(allocator);
        ) {
            logger.info("Loading testdata1.csv into server");
            TestData.loadTestDataIntoVsc(vsc, "testdata1.csv");
            clientAPI.put(cachePath1, vsc);
            vsc.clear();

            logger.info("Loading testdata2.csv into server");
            TestData.loadTestDataIntoVsc(vsc, "testdata2.csv");
            clientAPI.put(cachePath1, vsc);
            vsc.clear();

            logger.info("Loading testdata3.csv into server");
            TestData.loadTestDataIntoVsc(vsc, "testdata3.csv");
            clientAPI.put(cachePath2, vsc);
            vsc.clear();

            logger.info("Running query for path: {} and filters: {}", cachePath1, TestData.FILTERS1);
            clientAPI.get(cachePath1, TestData.FILTERS1, LISTENER);

            logger.info("Running query for path: {} and filters: {}", cachePath1, TestData.FILTERS2);
            clientAPI.get(cachePath1, TestData.FILTERS2, LISTENER);

            logger.info("Running query for path: {} and filters: {}", cachePath1, TestData.FILTERS3);
            clientAPI.get(cachePath1, TestData.FILTERS3, LISTENER);

            logger.info("Running query for path: {} and filters: {}", cachePath1, TestData.FILTERS4);
            clientAPI.get(cachePath1, TestData.FILTERS4, LISTENER);

            logger.info("Running query for path: {} and filters: {}", cachePath2, TestData.FILTERS1);
            clientAPI.get(cachePath2, TestData.FILTERS1, LISTENER);

            logger.info("Running query for path: {} and filters: {}", cachePath2, TestData.FILTERS2);
            clientAPI.get(cachePath2, TestData.FILTERS2, LISTENER);

            logger.info("Running query for path: {} and filters: {}", cachePath2, TestData.FILTERS3);
            clientAPI.get(cachePath2, TestData.FILTERS3, LISTENER);

            logger.info("Running query for path: {} and filters: {}", cachePath2, TestData.FILTERS4);
            clientAPI.get(cachePath2, TestData.FILTERS4, LISTENER);

            logger.info("Deleting entries for path: {} and filters: {}", cachePath2, TestData.FILTERS4);
            clientAPI.remove(cachePath2, TestData.FILTERS4);

            logger.info("Running query for path: {} and filters: {}", cachePath2, TestData.FILTERS4);
            clientAPI.get(cachePath2, TestData.FILTERS4, LISTENER);

            logger.info("Done");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static ClientAPI.Listener LISTENER = new ClientAPI.Listener() {

        @Override
        public void onNext(VectorSchemaRoot vsc) {
            ArrowUtils.toLines(logger::info, vsc);
        }

        @Override
        public void onError(Throwable ex) {
            logger.error(ex.getMessage(), ex);
        }

        @Override
        public void onCompleted() {
            logger.info("Completed");
        }
    };
}
