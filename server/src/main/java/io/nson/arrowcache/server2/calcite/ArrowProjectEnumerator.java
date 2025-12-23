package io.nson.arrowcache.server2.calcite;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.util.*;

import java.util.List;

class ArrowProjectEnumerator extends AbstractArrowEnumerator {
    private final Projector projector;

    ArrowProjectEnumerator(
            Schema arrowSchema,
            List<ArrowRecordBatch> arrowRecordBatches,
            ImmutableIntList fields,
            Projector projector
    ) {
        super(arrowSchema, arrowRecordBatches, fields);
        this.projector = projector;
    }

    protected void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
        try {
            this.projector.evaluate(arrowRecordBatch, this.valueVectors);
        } catch (GandivaException e) {
            throw Util.toUnchecked(e);
        }
    }

    public boolean moveNext() {
        if (this.currRowIndex >= this.rowCount - 1) {
            final boolean hasNextBatch = this.hasNextBatch();

            if (hasNextBatch) {
                this.currRowIndex = 0;
                this.valueVectors.clear();
                this.loadNextArrowBatch();
            }

            return hasNextBatch;
        } else {
            ++this.currRowIndex;
            return true;
        }
    }

    public void close() {
        try {
            this.projector.close();
        } catch (GandivaException ex) {
            throw Util.toUnchecked(ex);
        }
        super.close();
    }
}
