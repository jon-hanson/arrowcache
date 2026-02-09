package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.server.cache.DataTable;
import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.SelectionVector;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class ArrowFilterEnumerator extends AbstractArrowEnumerator {
    private static final Logger logger = LoggerFactory.getLogger(ArrowFilterEnumerator.class);

    private final Filter filter;
    private @Nullable ArrowBuf buf;
    private @Nullable SelectionVector selectionVector;
    private int selectionVectorIndex;

    ArrowFilterEnumerator(
            BufferAllocator allocator,
            Schema arrowSchema,
            List<DataTable.Batch> arrowTableBatches,
            ImmutableIntList fields,
            Filter filter
    ) {
        super(allocator, arrowSchema, arrowTableBatches, fields);
        this.filter = filter;
    }

    @Override
    public void close() {
        logger.debug("Closing...");
        try {
            if (this.buf != null) {
                this.buf.close();
            }

            this.filter.close();

            super.close();
        } catch (GandivaException ex) {
            throw Util.toUnchecked(ex);
        }
    }

    @Override
    void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
        try {
            if (this.buf != null) {
                this.buf.close();
            }
            this.buf = this.allocator.buffer((long)this.rowCount * 2L);
            this.selectionVector = new SelectionVectorInt16(this.buf);
            this.filter.evaluate(arrowRecordBatch, this.selectionVector);
        } catch (GandivaException ex) {
            throw Util.toUnchecked(ex);
        }
    }

    @Override
    public boolean moveNext() {
        while (true) {
            if (this.selectionVector != null &&
                    this.selectionVectorIndex < this.selectionVector.getRecordCount()
            ) {
                if (!this.arrowTableBatches.get(this.currentBatchIndex).deleted().contains(this.selectionVectorIndex)) {
                    this.currRowIndex = this.selectionVector.getIndex(this.selectionVectorIndex++);
                    return true;
                } else {
                    this.selectionVectorIndex++;
                }
            } else {
                this.currentBatchIndex++;
                if (!hasNextBatch()) {
                    return false;
                } else {
                    this.selectionVectorIndex = 0;
                    this.loadNextArrowBatch();
                }
            }
        }
    }
}
