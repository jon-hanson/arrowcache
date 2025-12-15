package io.nson.arrowcache.client;

import io.nson.arrowcache.client.impl.ArrowFlightClientImpl;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.common.utils.ArrowUtils;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClientAPI {
    private static final Logger logger = LoggerFactory.getLogger(TestClientAPI.class);

    /*
     * --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
     */
    public static void main(String[] args) {

        final CachePath cachePath = CachePath.valueOf("abc", "def");
        final Location location = Location.forGrpcInsecure("localhost", 12233);

        try (
                final RootAllocator allocator = new RootAllocator();
                final ClientAPI clientAPI = ArrowFlightClientImpl.create(location);
                final VectorSchemaRoot vsc = TestData.createTestDataVSC(allocator);
        ) {
            logger.info("Loading testdata1.csv into server");
            TestData.loadTestDataIntoVsc(vsc, "testdata1.csv");
            clientAPI.put(cachePath, vsc);
            vsc.clear();

            logger.info("Running query for path: {} and filters: {}", TestData.FILTERS1, cachePath);

            clientAPI.get(cachePath, TestData.FILTERS1, new ClientAPI.Listener() {
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
                }
            });

            logger.info("Done");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
