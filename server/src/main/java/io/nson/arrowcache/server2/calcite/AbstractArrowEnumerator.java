package io.nson.arrowcache.server2.calcite;

import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.*;

import java.util.*;

abstract class AbstractArrowEnumerator implements Enumerator<Object>, AutoCloseable {
    protected final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    protected final List<ArrowRecordBatch> arrowRecordBatches;
    protected final VectorSchemaRoot vectorSchemaRoot;
    protected final List<Integer> fields;
    protected final List<ValueVector> valueVectors;
    protected int currentBatchIndex;
    protected int currRowIndex;
    protected int rowCount;

    AbstractArrowEnumerator(
            Schema arrowSchema,
            List<ArrowRecordBatch> arrowRecordBatches,
            ImmutableIntList fields
    ) {
        this.vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);
        this.arrowRecordBatches = arrowRecordBatches;
        this.fields = fields;
        this.valueVectors = new ArrayList<>(fields.size());
        this.currentBatchIndex = -1;
        this.currRowIndex = -1;
    }

    @Override
    public void close() {
        this.vectorSchemaRoot.close();
    }

    abstract void evaluateOperator(ArrowRecordBatch var1);

    protected boolean hasNextBatch() {
        return this.currentBatchIndex < this.arrowRecordBatches.size();
    }

    protected void loadNextArrowBatch() {
        for(int i : this.fields) {
            this.valueVectors.add(this.vectorSchemaRoot.getVector(i));
        }

        this.rowCount = this.vectorSchemaRoot.getRowCount();

        this.evaluateOperator(this.arrowRecordBatches.get(this.currentBatchIndex));
    }

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

    public void reset() {
        throw new UnsupportedOperationException();
    }
}
