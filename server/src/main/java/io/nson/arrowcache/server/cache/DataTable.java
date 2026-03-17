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

        public void writeActiveRecords(VectorSchemaRoot tempVsc, VectorSchemaRoot targetVsc) {
            Objects.requireNonNull(arrowSchema);

            if (tempVsc.getRowCount() > 0) {
                tempVsc.allocateNew();
            }

            if (deleted.isEmpty()) {
                final VectorLoader loader = new VectorLoader(tempVsc);
                loader.load(arrowRecordBatch);
                VectorSchemaRootAppender.append(false, targetVsc, tempVsc);
            } else {
                final VectorLoader loader = new VectorLoader(tempVsc);
                loader.load(arrowRecordBatch);

                final VectorSchemaRoot[] vscSlices =
                        CollectionUtils.rangesFromExcluded(
                                        arrowRecordBatch.getLength(),
                                        deleted
                                ).stream()
                                .map(range -> tempVsc.slice(range.start(), range.length()))
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
//
//        public void writeActiveRecords(
//                VectorSchemaRoot tempVsc,
//                VectorSchemaRoot targetVsc,
//                int batchSize
//        ) {
//            Objects.requireNonNull(arrowSchema);
//
//            if (tempVsc.getRowCount() > 0) {
//                tempVsc.allocateNew();
//            }
//
//            final VectorLoader loader = new VectorLoader(tempVsc);
//            loader.load(arrowRecordBatch);
//            VectorSchemaRootAppender.append(false, targetVsc, tempVsc);
//        }

        List<VectorSchemaRoot> splitIntoSlices(int sliceSize, VectorSchemaRoot tempVsc) {
            Objects.requireNonNull(arrowSchema);

            assert(deleted.isEmpty());

            final VectorLoader loader = new VectorLoader(tempVsc);
            loader.load(arrowRecordBatch);

            return CollectionUtils.generateSlices(arrowRecordBatch.getLength(), sliceSize)
                    .stream()
                    .map(range -> tempVsc.slice(range.start(), range.length()))
                    .collect(Collectors.toList());
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
        allocator.close();
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

    public void addBatches(Schema newSchema, Collection<ArrowRecordBatch> newBatches) {
        if (arrowSchema == null) {
            arrowSchema = newSchema;
            keyColumnIndex = ArrowServerUtils.findKeyColumn(newSchema, keyColumnName)
                    .orElseThrow(() ->
                            ExceptionUtils.exception(
                                    logger,
                                    "Key column name '" + keyColumnName + "' not found in schema"
                            ).create(IllegalArgumentException::new)
                    );
        } else if (!arrowSchema.equals(newSchema)) {
            throw ExceptionUtils.exception(
                    logger,
                    "Cannot add batches with a different schema"
            ).create(IllegalArgumentException::new);
        }

        synchronized (rwLock.writeLock()) {
            try (VectorSchemaRoot vsc = VectorSchemaRoot.create(arrowSchema, allocator)) {
                final VectorLoader loader = new VectorLoader(vsc);
                for (final ArrowRecordBatch batch : newBatches) {
                    loader.load(batch);
                    batches.add(new Batch(batch));
                    updateRowCoordMap(batches.size() - 1, vsc);
                }
            }
        }
    }

    private void updateRowCoordMap(int batchIndex, VectorSchemaRoot vsc) {
        Objects.requireNonNull(keyColumnIndex);

        final int rowCount = vsc.getRowCount();
        final FieldVector fv = vsc.getFieldVectors().get(keyColumnIndex);
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            final Object key = fv.getObject(rowIndex);
            final RowCoordinate oldRowCoordinate = rowCoordinateMap.get(key);
            if (oldRowCoordinate != null) {
                batches.get(oldRowCoordinate.batchIndex).markAsDeleted(oldRowCoordinate.rowIndex);
            }
            rowCoordinateMap.put(key, new RowCoordinate(batchIndex, rowIndex));
        }
    }

    public void get(Set<?> keys, FlightProducer.ServerStreamListener listener) {
        synchronized (rwLock.readLock()) {
            if (arrowSchema == null) {
                try (VectorSchemaRoot vsc = VectorSchemaRoot.create(ArrowServerUtils.EMPTY_SCHEMA, allocator)) {
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
        Objects.requireNonNull(arrowSchema);

        synchronized (rwLock.readLock()) {
            try (
                    final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(arrowSchema, allocator);
                    final VectorSchemaRoot batchVsc = VectorSchemaRoot.create(arrowSchema, allocator)
            ) {
                resultVsc.allocateNew();

                final VectorLoader loader = new VectorLoader(batchVsc);

                listener.start(resultVsc);

                // Extract the matched rows for each batch and write back to the listener.
                batchMatches.forEach((batchIndex, matches) -> {
                    final Batch batch = batches.get(batchIndex);
                    loader.load(batch.arrowRecordBatch);
                    VectorSchemaRoot[] vecSlices = null;

                    try {
                        final List<CollectionUtils.Range> ranges =
                                CollectionUtils.rangesFromIncluded(matches);

                        vecSlices = ranges.stream()
                                .filter(range -> range.length() > 0)
                                .map(range -> batchVsc.slice(range.start(), range.length()))
                                .toArray(VectorSchemaRoot[]::new);

                        if (vecSlices.length > 0) {
                            if (resultVsc.getRowCount() > 0) {
                                resultVsc.allocateNew();
                            }

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

    public void mergeBatches(OptionalInt batchSizeOpt) {
        if (batchSizeOpt.isPresent()) {
            mergeBatches(batchSizeOpt.getAsInt());
        } else {
            mergeBatches();
        }
    }

    public void mergeBatches() {
        if (batches.size() > 1) {
            logger.info("Merging {} batches into 1 for table {}", batches.size(), name);
            mergeBatchesImpl(true);
        } else {
            logger.warn("Only one batch, merging skipped for table '{}'" , name);
        }
    }

    private void mergeBatchesImpl(boolean updateRowCoordMap) {
        if (batches.size() > 1) {
            Objects.requireNonNull(arrowSchema);

            logger.info("Merging {} batches into a single batch", batches.size());

            try (
                    final VectorSchemaRoot tempVsc = VectorSchemaRoot.create(arrowSchema, allocator);
                    final VectorSchemaRoot mergedVsc = VectorSchemaRoot.create(arrowSchema, allocator)
            ) {
                synchronized (rwLock.writeLock()) {
                    mergedVsc.allocateNew();

                    // Merge the active records from each batch into a single VectorSchemaRoot.
                    for (Batch batch : batches) {
                        batch.writeActiveRecords(tempVsc, mergedVsc);
                    }

                    // Create a new batch from the merged VectorSchemaRoot.
                    final VectorUnloader unloader = new VectorUnloader(mergedVsc);
                    batches.forEach(Batch::close);
                    batches.clear();
                    batches.add(new Batch(unloader.getRecordBatch()));

                    if (updateRowCoordMap) {
                        // Regenerate the row coordinate map.
                        rowCoordinateMap.clear();
                        updateRowCoordMap(0, mergedVsc);
                    }
                }
            }
        }
    }

    public void mergeBatches(int batchSize) {
        if (batches.isEmpty()) {
            logger.warn("No batches to merge for table '{}'", name);
        } else {
            logger.info("Merging {} batches into batches of size {} for table '{}'" ,batches.size(), batchSize,  name);

            Objects.requireNonNull(arrowSchema);

            // First merge the batches into a single batch.
            mergeBatchesImpl(false);

            assert(batches.size() == 1);

            final int currBatchSize = batches.get(0).arrowRecordBatch.getLength();
            logger.info("Intermediate merged batch has size {}", currBatchSize);

            if (currBatchSize <= batchSize) {
                logger.info("Single batch is smaller than requested batch size {}, nothing to do", batchSize);
            } else {
                logger.info("Splitting single batch into batches of size {}", batchSize);

                try (
                        final VectorSchemaRoot tempVsc = VectorSchemaRoot.create(arrowSchema, allocator)
                ) {
                    final List<VectorSchemaRoot> vscSlices = batches.get(0).splitIntoSlices(batchSize, tempVsc);
                    try {
                        batches.forEach(Batch::close);
                        batches.clear();
                        rowCoordinateMap.clear();

                        vscSlices.forEach(vecSlice -> {
                            final VectorUnloader unloader = new VectorUnloader(vecSlice);
                            addBatch(arrowSchema, unloader.getRecordBatch());
                            vecSlice.close();
                        });

                        logger.info("Finished splitting, now have {} batches", batches.size());
                    } finally {
                        for (VectorSchemaRoot vscSlice : vscSlices) {
                            vscSlice.close();
                        }
                    }
                }
            }
        }
    }
}
