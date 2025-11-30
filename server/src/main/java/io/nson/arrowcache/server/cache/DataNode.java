package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.server.utils.MultiConsumer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

public interface DataNode extends AutoCloseable {

    class LeafDataNode implements DataNode {
        private final Schema schema;
        private final List<ArrowRecordBatch> batches;

        public LeafDataNode(Schema schema, List<ArrowRecordBatch> batches) {
            this.schema = schema;
            this.batches = batches;
        }

        public LeafDataNode(Schema schema) {
            this(schema, new ArrayList<>());
        }

        @Override
        public void close() throws Exception {
            for (ArrowRecordBatch batch : batches) {
                batch.close();
            }
        }

        public void add(Collection<ArrowRecordBatch> batches) {
            this.batches.addAll(batches);
        }

        @Override
        public void execute(
                CachePath path,
                Api.Query query,
                MultiConsumer<ArrowRecordBatch> batchConsumer
        ) {

        }
    }

    class ParentDataNode implements DataNode {
        private final Optional<Schema> schema;
        private final Map<String, DataNode> childNodes;

        private ParentDataNode(
                Optional<Schema> schema,
                Map<String, DataNode> childNodes
        ) {
            this.schema = schema;
            this.childNodes = childNodes;
        }

        private ParentDataNode(
                Schema schema,
                Map<String, DataNode> childNodes
        ) {
            this(Optional.of(schema), childNodes);
        }

        @Override
        public void close() throws Exception {
            for (DataNode childNode : childNodes.values()) {
                childNode.close();
            }
        }

        @Override
        public void execute(
                CachePath path,
                Api.Query query,
                MultiConsumer<ArrowRecordBatch> batchConsumer
        ) {

        }
    }

    void execute(
            CachePath path,
            Api.Query query,
            MultiConsumer<ArrowRecordBatch> batchConsumer
    );
}
