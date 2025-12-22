package io.nson.arrowcache.server2;

import org.apache.arrow.gandiva.evaluator.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.util.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

class ArrowEnumerable extends AbstractEnumerable<Object> {
    private final Schema arrowSchema;
    private final List<ArrowRecordBatch> arrowRecordBatches;
    private final ImmutableIntList fields;
    private final @Nullable Projector projector;
    private final @Nullable Filter filter;

    ArrowEnumerable(
            Schema arrowSchema,
            List<ArrowRecordBatch> arrowRecordBatches,
            ImmutableIntList fields,
            @Nullable Projector projector,
            @Nullable Filter filter
    ) {
        this.arrowSchema = arrowSchema;
        this.arrowRecordBatches = arrowRecordBatches;
        this.projector = projector;
        this.filter = filter;
        this.fields = fields;
    }

    public Enumerator<Object> enumerator() {
        try {
            if (this.projector != null) {
                return new ArrowProjectEnumerator(this.arrowSchema, arrowRecordBatches, this.fields, this.projector);
            } else if (this.filter != null) {
                return new ArrowFilterEnumerator(this.arrowSchema, arrowRecordBatches, this.fields, this.filter);
            } else {
                throw new IllegalArgumentException("The arrow enumerator must have either a filter or a projection");
            }
        } catch (Exception e) {
            throw Util.toUnchecked(e);
        }
    }
}
