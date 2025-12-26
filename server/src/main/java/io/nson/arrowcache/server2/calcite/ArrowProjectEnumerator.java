package io.nson.arrowcache.server2.calcite;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ArrowProjectEnumerator extends AbstractArrowEnumerator {
    private static final Logger logger = LoggerFactory.getLogger(ArrowProjectEnumerator.class);

    private final Projector projector;

    ArrowProjectEnumerator(
            BufferAllocator allocator,
            Schema arrowSchema,
            List<ArrowTable.Batch> arrowTableBatches,
            ImmutableIntList fields,
            Projector projector
    ) {
        super(allocator, arrowSchema, arrowTableBatches, fields);
        this.projector = projector;
    }

    @Override
    public void close() {
        logger.info("Closing...");
        try {
            this.projector.close();
        } catch (GandivaException ex) {
            throw Util.toUnchecked(ex);
        }
        super.close();
    }

    protected void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
        try {
            this.projector.evaluate(arrowRecordBatch, this.valueVectors);
        } catch (GandivaException e) {
            throw Util.toUnchecked(e);
        }
    }

    @Override
    public boolean moveNext() {
        while (true) {
            if (this.currRowIndex >= this.rowCount - 1) {
                if (hasNextBatch()) {
                    this.currentBatchIndex++;
                    this.currRowIndex = 0;
                    this.loadNextArrowBatch();
                } else {
                    return false;
                }
            } else {
                ++this.currRowIndex;
            }

            if (!arrowTableBatches.get(this.currentBatchIndex).deleted().contains(this.currRowIndex)) {
                return true;
            }
        }
    }

    protected boolean nextRow() {
        if (this.currRowIndex >= this.rowCount - 1) {
            if (hasNextBatch()) {
                loadNextArrowBatch();
                return true;
            } else {
                return false;
            }
        } else {
            ++this.currRowIndex;
            return true;
        }
    }
}
