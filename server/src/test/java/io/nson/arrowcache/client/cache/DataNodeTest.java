package io.nson.arrowcache.client.cache;

import io.nson.arrowcache.client.TestData;
import io.nson.arrowcache.server.cache.DataNode;
import io.nson.arrowcache.server.utils.ArrowUtils;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(DataNodeTest.class);

    public static void main(String[] args) throws IOException {

        try(
                final BufferAllocator allocator = new RootAllocator();
                final VectorSchemaRoot vsc = TestData.createTestDataVSC(allocator);
                final DataNode dataNode = new DataNode(allocator, vsc.getSchema(), "id");
        ) {
            final VectorUnloader unloader = new VectorUnloader(vsc);

            logger.info("Loading testdata1.csv");
            TestData.loadTestData(vsc, "testdata1.csv");
            dataNode.add(unloader.getRecordBatch());

            logger.info("Loading testdata2.csv");
            TestData.loadTestData(vsc, "testdata2.csv");
            dataNode.add(unloader.getRecordBatch());

            final ResultListener resultListener = new ResultListener();

            logger.info("Testing QUERY1: {}", TestData.QUERY1);
            dataNode.execute(TestData.QUERY1, resultListener);

            logger.info("Testing QUERY2: {}", TestData.QUERY2);
            dataNode.execute(TestData.QUERY2, resultListener);

            logger.info("Testing QUERY3: {}", TestData.QUERY3);
            dataNode.execute(TestData.QUERY3, resultListener);

            logger.info("Testing QUERY4: {}", TestData.QUERY4);
            dataNode.execute(TestData.QUERY4, resultListener);
        }
    }

    private static class ResultListener implements FlightProducer.ServerStreamListener {
        VectorSchemaRoot vsc;
        VectorUnloader unloader;

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
            this.unloader = new VectorUnloader(root);
        }

        @Override
        public void putNext() {
            ArrowUtils.toLines(logger::info, vsc);
        }

        @Override
        public void putNext(ArrowBuf metadata) {

        }

        @Override
        public void putMetadata(ArrowBuf metadata) {

        }

        @Override
        public void error(Throwable ex) {

        }

        @Override
        public void completed() {

        }
    }
}
