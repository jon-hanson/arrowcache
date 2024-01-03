package io.nson.arrowcache.server;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.server.utils.MultiConsumer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

public interface DataNode {

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

        public void add(Collection<ArrowRecordBatch> batches) {
            this.batches.addAll(batches);
        }

        @Override
        public void execute(Api.Query query, MultiConsumer<ArrowRecordBatch> batchConsumer) {

        }
    }

    class ParentDataNode implements DataNode {
        private final List<Api.Filter> filters;
        private final List<DataNode> childNodes;

        public ParentDataNode(List<Api.Filter> filters, List<DataNode> childNodes) {
            this.filters = filters;
            this.childNodes = childNodes;
        }

        public ParentDataNode(List<Api.Filter> filters) {
            this(filters, new ArrayList<>());
        }

        @Override
        public void execute(Api.Query query, MultiConsumer<ArrowRecordBatch> batchConsumer) {

        }
    }

    void execute(Api.Query query, MultiConsumer<ArrowRecordBatch> batchConsumer);
}
