package io.nson.arrowcache.server2.calcite;

import org.apache.arrow.gandiva.evaluator.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.util.*;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ArrowEnumerable extends AbstractEnumerable<Object> {
    private static final Logger logger = LoggerFactory.getLogger(ArrowEnumerable.class);

    private final BufferAllocator allocator;
    private final Schema arrowSchema;
    private final List<ArrowTable.Batch> arrowTableBatches;
    private final ImmutableIntList fields;
    private final @Nullable Projector projector;
    private final @Nullable Filter filter;

    ArrowEnumerable(
            BufferAllocator allocator,
            Schema arrowSchema,
            List<ArrowTable.Batch> arrowTableBatches,
            ImmutableIntList fields,
            @Nullable Projector projector,
            @Nullable Filter filter
    ) {
        this.allocator = allocator;
        this.arrowSchema = arrowSchema;
        this.arrowTableBatches = arrowTableBatches;
        this.projector = projector;
        this.filter = filter;
        this.fields = fields;
    }

    public Enumerator<Object> enumerator() {
        try {
            if (this.projector != null) {
                return new ArrowProjectEnumerator(
                        this.allocator,
                        this.arrowSchema,
                        this.arrowTableBatches,
                        this.fields,
                        this.projector
                );
            } else if (this.filter != null) {
                return new ArrowFilterEnumerator(
                        this.allocator,
                        this.arrowSchema,
                        arrowTableBatches,
                        this.fields,
                        this.filter
                );
            } else {
                throw ExceptionUtils.logError(
                        logger,
                        IllegalArgumentException::new,
                        "The arrow enumerator must have either a filter or a projection"
                );
            }
        } catch (Exception e) {
            throw Util.toUnchecked(e);
        }
    }
}
