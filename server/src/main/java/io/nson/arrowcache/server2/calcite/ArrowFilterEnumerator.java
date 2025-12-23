package io.nson.arrowcache.server2.calcite;

import org.apache.arrow.gandiva.evaluator.*;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.util.*;
import org.jspecify.annotations.Nullable;

import java.util.*;

class ArrowFilterEnumerator extends AbstractArrowEnumerator {
    private final Filter filter;
    private @Nullable ArrowBuf buf;
    private @Nullable SelectionVector selectionVector;
    private int selectionVectorIndex;

    ArrowFilterEnumerator(
            Schema arrowSchema,
            List<ArrowRecordBatch> arrowRecordBatches,
            ImmutableIntList fields,
            Filter filter
    ) {
        super(arrowSchema, arrowRecordBatches, fields);
        this.filter = filter;
    }

    void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
        try {
            this.buf = this.allocator.buffer((long)this.rowCount * 2L);
            this.selectionVector = new SelectionVectorInt16(this.buf);
            this.filter.evaluate(arrowRecordBatch, this.selectionVector);
        } catch (GandivaException ex) {
            throw Util.toUnchecked(ex);
        }
    }

    public boolean moveNext() {
        if (this.selectionVector != null && this.selectionVectorIndex < this.selectionVector.getRecordCount()) {
            this.currRowIndex = this.selectionVector.getIndex(this.selectionVectorIndex++);
            return true;
        } else {
            boolean hasNextBatch;
            while(true) {
                this.currentBatchIndex++;
                hasNextBatch = hasNextBatch();
                if (!hasNextBatch) {
                    break;
                }

                this.selectionVectorIndex = 0;
                this.valueVectors.clear();
                this.loadNextArrowBatch();

                if (this.selectionVectorIndex < this.selectionVector.getRecordCount()) {
                    this.currRowIndex = this.selectionVector.getIndex(this.selectionVectorIndex++);
                    break;
                }
            }

            return hasNextBatch;
        }
    }

    public void close() {
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
}
