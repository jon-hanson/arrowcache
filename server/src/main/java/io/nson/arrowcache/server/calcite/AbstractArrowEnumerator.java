package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.server.cache.DataTable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.ImmutableIntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractArrowEnumerator implements Enumerator<Object> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractArrowEnumerator.class);

    protected final BufferAllocator allocator;
    protected final List<DataTable.Batch> arrowTableBatches;
    protected final VectorSchemaRoot vectorSchemaRoot;
    protected final VectorLoader loader;
    protected final List<Integer> fields;
    protected final List<ValueVector> valueVectors;
    protected int currentBatchIndex;
    protected int currRowIndex;
    protected int rowCount;

    AbstractArrowEnumerator(
            BufferAllocator allocator,
            Schema arrowSchema,
            List<DataTable.Batch> arrowTableBatches,
            ImmutableIntList fields
    ) {
        this.allocator = allocator;
        this.vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);
        this.loader = new VectorLoader(vectorSchemaRoot);
        this.arrowTableBatches = arrowTableBatches;
        this.fields = fields;
        this.valueVectors = new ArrayList<>(fields.size());
        this.currentBatchIndex = -1;
        this.currRowIndex = -1;
    }

    @Override
    public void close() {
        logger.debug("Closing...");
        this.vectorSchemaRoot.close();
    }

    abstract void evaluateOperator(ArrowRecordBatch var1);

    protected boolean hasNextBatch() {
        return this.currentBatchIndex < this.arrowTableBatches.size();
    }

    protected void loadNextArrowBatch() {
        final DataTable.Batch tableBatch = this.arrowTableBatches.get(this.currentBatchIndex);
        final ArrowRecordBatch arrowRecordBatch = tableBatch.arrowRecordBatch();

        this.loader.load(arrowRecordBatch);

        this.valueVectors.clear();
        for (int i : this.fields) {
            this.valueVectors.add(this.vectorSchemaRoot.getVector(i));
        }

        this.rowCount = this.vectorSchemaRoot.getRowCount() - tableBatch.deleted().size();

        this.evaluateOperator(arrowRecordBatch);
    }

    @Override
    public Object current() {
        if (this.fields.size() == 1) {
            return this.valueVectors.get(0).getObject(this.currRowIndex);
        } else {
            final Object[] current = new Object[this.valueVectors.size()];

            for(int i = 0; i < this.valueVectors.size(); ++i) {
                final ValueVector vector = this.valueVectors.get(i);
                current[i] = vector.getObject(this.currRowIndex);
            }

            return current;
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }
}
