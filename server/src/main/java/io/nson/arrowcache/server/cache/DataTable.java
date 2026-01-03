package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.utils.ExceptionUtils;
import io.nson.arrowcache.server.SchemaConfig;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

public class DataTable implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DataTable.class);

    private static final class RowCoordinate {
        private final int batchIndex;
        private final int rowIndex;

        private RowCoordinate(int batchIndex, int rowIndex) {
            this.batchIndex = batchIndex;
            this.rowIndex = rowIndex;
        }

        public int getBatchIndex() {
            return batchIndex;
        }

        public int getRowIndex() {
            return rowIndex;
        }
    }

    public static final class Batch implements AutoCloseable {
        private static final Logger logger = LoggerFactory.getLogger(Batch.class);

        private final ArrowRecordBatch arrowRecordBatch;
        private final Set<Integer> deleted;

        private Batch(ArrowRecordBatch arrowRecordBatch, Set<Integer> deleted) {
            this.arrowRecordBatch = arrowRecordBatch;
            this.deleted = deleted;
        }

        private Batch(ArrowRecordBatch batch) {
            this(batch, new HashSet<>());
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
    }

    private static int findKeyColumn(Schema schema, String keyName) {
        final List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i).getName().equals(keyName)) {
                return i;
            }
        }

        logger.error("Key column name '{}' not found in schema", keyName);
        throw new RuntimeException("Key column name '" + keyName + "' not found in schema");
    }

    private final BufferAllocator allocator;
    private final String name;
    private @Nullable Schema arrowSchema;
    private final String keyColumnName;
    private @Nullable int keyColumnIndex;
    private final List<Batch> batches;
    private final Map<Object, RowCoordinate> rowCoordinateMap;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public DataTable(
            BufferAllocator allocator,
            String name,
            SchemaConfig.TableConfig tableConfig,
            Schema arrowSchema
    ) {
        this.allocator = allocator.newChildAllocator(name, 0, Long.MAX_VALUE);
        this.name = name;
        this.arrowSchema = arrowSchema;
        this.keyColumnName = tableConfig.keyColumn();
        this.batches = new ArrayList<>();
        this.rowCoordinateMap = new HashMap<>();
    }

    public DataTable(
            BufferAllocator allocator,
            String name,
            SchemaConfig.TableConfig tableConfig
    ) {
        this(allocator, name, tableConfig, null);
    }

    @Override
    public void close() {
        logger.info("Closing...");
        batches.forEach(Batch::close);
        this.allocator.close();
    }

    public String name() {
        return name;
    }

    public Optional<Schema> arrowSchema() {
        return Optional.ofNullable(arrowSchema);
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
            this.keyColumnIndex = findKeyColumn(arrowSchema, keyColumnName);
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
                    processBatch(this.batches.size() - 1, vsc);
                }
            }
        }
    }

    private void processBatch(int batchIndex, VectorSchemaRoot vsc) {
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
            final Map<Integer, Set<Integer>> matches = keys.stream()
                    .filter(rowCoordinateMap::containsKey)
                    .map(rowCoordinateMap::get)
                    .collect(Collectors.groupingBy(
                            RowCoordinate::getBatchIndex,
                            mapping(RowCoordinate::getRowIndex, toSet())
                    ));

            getImpl(matches, listener);
        }
    }

    protected void getImpl(
            Map<Integer, Set<Integer>> batchMatches,
            FlightProducer.ServerStreamListener listener
    ) {
        synchronized (this.rwLock.readLock()) {
            try (
                    final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(this.arrowSchema, this.allocator);
                    final VectorSchemaRoot vsc = VectorSchemaRoot.create(this.arrowSchema, this.allocator)
            ) {
                final VectorLoader loader = new VectorLoader(vsc);

                listener.start(resultVsc);

                batchMatches.forEach((batchIndex, matches) -> {
                    final Batch batch = this.batches.get(batchIndex);
                    loader.load(batch.arrowRecordBatch);
                    VectorSchemaRoot[] slices = null;

                    try {
                        slices = matches.stream()
                                .map(i -> vsc.slice(i, 1))
                                .toArray(VectorSchemaRoot[]::new);

                        resultVsc.allocateNew();
                        VectorSchemaRootAppender.append(false, resultVsc, slices);
                        listener.putNext();
                    } finally {
                        if (slices != null) {
                            for (VectorSchemaRoot slice : slices) {
                                slice.close();
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
}
