package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.common.utils.ExceptionUtils;
import io.nson.arrowcache.server.RootSchemaConfig;
import io.nson.arrowcache.server.utils.ArrowServerUtils;
import io.nson.arrowcache.server.utils.CollectionUtils;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toCollection;

@NullMarked
public class DataTable implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DataTable.class);

    public static DataTable create(
            BufferAllocator allocator,
            String name,
            RootSchemaConfig.TableConfig tableConfig,
            Schema arrowSchema
    ) {
        return new DataTable(allocator, name, tableConfig, arrowSchema);
    }

    public static DataTable create(
            BufferAllocator allocator,
            String name,
            RootSchemaConfig.TableConfig tableConfig
    ) {
        return new DataTable(allocator, name, tableConfig, null);
    }

    private static final class RowCoordinate {
        private final int batchIndex;
        private final int rowIndex;

        private RowCoordinate(int batchIndex, int rowIndex) {
            this.batchIndex = batchIndex;
            this.rowIndex = rowIndex;
        }

        int getBatchIndex() {
            return batchIndex;
        }

        int getRowIndex() {
            return rowIndex;
        }
    }

    public final class Batch implements AutoCloseable {

        private final ArrowRecordBatch arrowRecordBatch;
        private final SortedSet<Integer> deleted;

        private Batch(ArrowRecordBatch arrowRecordBatch, SortedSet<Integer> deleted) {
            this.arrowRecordBatch = arrowRecordBatch;
            this.deleted = deleted;
        }

        private Batch(ArrowRecordBatch batch) {
            this(batch, new TreeSet<>());
        }

        @Override
        public void close() {
            logger.debug("Closing...");
            arrowRecordBatch.close();
        }

        public ArrowRecordBatch arrowRecordBatch() {
            return arrowRecordBatch;
        }

        public Set<Integer> deleted() {
            return deleted;
        }

        public void markAsDeleted(int rowIndex) {
            deleted.add(rowIndex);
        }

        public void writeActiveRecords(VectorSchemaRoot targetVsc) {
            Objects.requireNonNull(arrowSchema);

            if (deleted.isEmpty()) {
                try (final VectorSchemaRoot vsc = VectorSchemaRoot.create(arrowSchema, allocator)) {
                    final VectorLoader loader = new VectorLoader(vsc);
                    loader.load(arrowRecordBatch);
                    VectorSchemaRootAppender.append(false, targetVsc, vsc);
                }
            } else {
                try (final VectorSchemaRoot vsc = VectorSchemaRoot.create(arrowSchema, allocator)) {
                    final VectorLoader loader = new VectorLoader(vsc);
                    loader.load(arrowRecordBatch);

                    final VectorSchemaRoot[] vscSlices =
                            CollectionUtils.slicesFromExcluded(
                                            arrowRecordBatch.getLength(),
                                            deleted
                                    ).stream()
                                    .map(slice -> vsc.slice(slice.start(), slice.length()))
                                    .toArray(VectorSchemaRoot[]::new);

                    try {
                        VectorSchemaRootAppender.append(false, targetVsc, vscSlices);
                    } finally {
                        for (VectorSchemaRoot vscSlice : vscSlices) {
                            vscSlice.close();
                        }
                    }
                }
            }
        }
    }

    private final BufferAllocator allocator;
    private final String name;
    private @Nullable Schema arrowSchema;
    private final String keyColumnName;
    private @Nullable Integer keyColumnIndex;
    private final List<Batch> batches;
    private final Map<Object, RowCoordinate> rowCoordinateMap;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private DataTable(
            BufferAllocator allocator,
            String name,
            RootSchemaConfig.TableConfig tableConfig,
            @Nullable Schema arrowSchema
    ) {
        this.allocator = allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
        this.name = Objects.requireNonNull(name);
        this.arrowSchema = arrowSchema;
        this.keyColumnName = Objects.requireNonNull(tableConfig.keyColumn());
        this.batches = new ArrayList<>();
        this.rowCoordinateMap = new HashMap<>();
    }

    @Override
    public void close() {
        logger.info("Closing {}...", name);
        batches.forEach(Batch::close);
        this.allocator.close();
    }

    public String name() {
        return name;
    }

    public Schema arrowSchema() {
        return arrowSchema != null ? arrowSchema : ArrowUtils.EMPTY_SCHEMA;
    }

    public List<Batch> arrowBatches() {
        return batches;
    }

    public Set<Object> keys() {
        return rowCoordinateMap.keySet();
    }

    public void addBatch(Schema arrowSchema, ArrowRecordBatch arb) {
        addBatches(arrowSchema, Collections.singletonList(arb));
    }

    public void addBatches(Schema arrowSchema, Collection<ArrowRecordBatch> batches) {
        if (this.arrowSchema == null) {
            this.arrowSchema = arrowSchema;
            this.keyColumnIndex = ArrowServerUtils.findKeyColumn(arrowSchema, keyColumnName)
                    .orElseThrow(() ->
                            ExceptionUtils.exception(
                                    logger,
                                    "Key column name '" + keyColumnName + "' not found in schema"
                            ).create(IllegalArgumentException::new)
                    );
        } else if (!this.arrowSchema.equals(arrowSchema)) {
            throw ExceptionUtils.exception(
                    logger,
                    "Cannot add batches with a different schema"
            ).create(IllegalArgumentException::new);
        }

        synchronized (this.rwLock.writeLock()) {
            try (VectorSchemaRoot vsc = VectorSchemaRoot.create(this.arrowSchema, this.allocator)) {
                final VectorLoader loader = new VectorLoader(vsc);
                for (final ArrowRecordBatch batch : batches) {
                    loader.load(batch);
                    this.batches.add(new Batch(batch));
                    updateRowCoordMap(this.batches.size() - 1, vsc);
                }
            }
        }
    }

    private void updateRowCoordMap(int batchIndex, VectorSchemaRoot vsc) {
        Objects.requireNonNull(this.keyColumnIndex);

        final int rowCount = vsc.getRowCount();
        final FieldVector fv = vsc.getFieldVectors().get(this.keyColumnIndex);
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            final Object key = fv.getObject(rowIndex);
            final RowCoordinate oldRowCoordinate = this.rowCoordinateMap.get(key);
            if (oldRowCoordinate != null) {
                batches.get(oldRowCoordinate.batchIndex).markAsDeleted(oldRowCoordinate.rowIndex);
            }
            this.rowCoordinateMap.put(key, new RowCoordinate(batchIndex, rowIndex));
        }
    }

    public void get(Set<?> keys, FlightProducer.ServerStreamListener listener) {
        synchronized (rwLock.readLock()) {
            if (this.arrowSchema == null) {
                try (VectorSchemaRoot vsc = VectorSchemaRoot.create(ArrowServerUtils.EMPTY_SCHEMA, this.allocator)) {
                    listener.start(vsc);
                    listener.completed();
                }
            } else {
                final Map<Integer, SortedSet<Integer>> matches = keys.stream()
                        .filter(rowCoordinateMap::containsKey)
                        .map(rowCoordinateMap::get)
                        .collect(Collectors.groupingBy(
                                RowCoordinate::getBatchIndex,
                                mapping(RowCoordinate::getRowIndex, toCollection(TreeSet::new))
                        ));

                getImpl(matches, listener);
            }
        }
    }

    protected void getImpl(
            Map<Integer, SortedSet<Integer>> batchMatches,
            FlightProducer.ServerStreamListener listener
    ) {
        Objects.requireNonNull(this.arrowSchema);

        synchronized (this.rwLock.readLock()) {
            try (
                    final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(this.arrowSchema, this.allocator);
                    final VectorSchemaRoot batchVsc = VectorSchemaRoot.create(this.arrowSchema, this.allocator)
            ) {
                resultVsc.allocateNew();

                final VectorLoader loader = new VectorLoader(batchVsc);

                listener.start(resultVsc);

                // Extract the matched rows for each batch and write back to the listener.
                batchMatches.forEach((batchIndex, matches) -> {
                    final Batch batch = this.batches.get(batchIndex);
                    loader.load(batch.arrowRecordBatch);
                    VectorSchemaRoot[] vecSlices = null;

                    try {
                        final List<CollectionUtils.Slice> slices =
                                CollectionUtils.slicesFromIncluded(matches);

                        vecSlices = slices.stream()
                                .filter(slice -> slice.length() > 0)
                                .map(slice -> batchVsc.slice(slice.start(), slice.length()))
                                .toArray(VectorSchemaRoot[]::new);

                        if (vecSlices.length > 0) {
                            VectorSchemaRootAppender.append(false, resultVsc, vecSlices);
                            listener.putNext();
                        }
                    } finally {
                        if (vecSlices != null) {
                            for (VectorSchemaRoot vecSlice : vecSlices) {
                                vecSlice.close();
                            }
                        }
                    }
                });
            }
        }
    }

    public void deleteEntries(Collection<Object> keys) {
        synchronized (rwLock.writeLock()) {
            keys.forEach(key -> {
                final RowCoordinate rowCoord = rowCoordinateMap.get(key);
                if (rowCoord != null) {
                    batches.get(rowCoord.batchIndex).markAsDeleted(rowCoord.rowIndex);
                }
            });
        }
    }

    public void mergeBatches() {
        if (this.batches.size() > 1) {
            Objects.requireNonNull(this.arrowSchema);

            logger.info("Merging {} batches into 1", this.batches.size());

            try (final VectorSchemaRoot mergedVsc = VectorSchemaRoot.create(this.arrowSchema, this.allocator)) {
                synchronized (this.rwLock.writeLock()) {
                    mergedVsc.allocateNew();

                    // Merge the active records from each batch into a single VectorSchemaRoot.
                    for (Batch batch : this.batches) {
                        batch.writeActiveRecords(mergedVsc);
                    }

                    // Regenerate the row coordinate map.
                    this.rowCoordinateMap.clear();
                    this.updateRowCoordMap(0, mergedVsc);

                    // Create a new batch from the merged VectorSchemaRoot.
                    final VectorUnloader unloader = new VectorUnloader(mergedVsc);
                    this.batches.forEach(Batch::close);
                    this.batches.clear();
                    this.batches.add(new Batch(unloader.getRecordBatch()));
                }
            }
        }
    }
}
