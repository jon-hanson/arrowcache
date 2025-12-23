package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.server.AllocatorManager;
import io.nson.arrowcache.server.QueryLogic;
import io.nson.arrowcache.server.utils.TranslateQuery;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class DataNode implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

    private static final TranslateQuery TRANSLATE_QUERY = new TranslateQuery(true, true);

    private static final class RowCoordinate {
        final int batchIndex;
        final int rowIndex;

        private RowCoordinate(int batchIndex, int rowIndex) {
            this.batchIndex = batchIndex;
            this.rowIndex = rowIndex;
        }
    }

    private final class Batch implements AutoCloseable {
        private final ArrowRecordBatch arrowRecordBatch;
        private final Set<Integer> replaced;

        private Batch(ArrowRecordBatch arrowRecordBatch, Set<Integer> replaced) {
            this.arrowRecordBatch = arrowRecordBatch;
            this.replaced = replaced;
        }

        private Batch(ArrowRecordBatch batch) {
            this(batch, new HashSet<>());
        }

        @Override
        public void close() {
            arrowRecordBatch.close();
        }

        public void markAsReplaced(int rowIndex) {
            replaced.add(rowIndex);
        }

        public Set<Integer> matches(
                VectorLoader loader,
                VectorSchemaRoot vsc,
                List<QueryLogic.Filter<?>> filters,
                int batchIndex
        ) {
            logger.info("Looking for matches in batch {}", batchIndex);

            loader.load(arrowRecordBatch);
            final Map<String, FieldVector> fvMap =
                    vsc.getFieldVectors().stream()
                            .collect(toMap(
                                    fv -> fv.getField().getName(),
                                    fv -> fv
                            ));

            Set<Integer> matches = null;

            boolean first = true;

            for (QueryLogic.Filter<?> filter : filters) {
                final String filterAttr = filter.attribute();
                final FieldVector fv = fvMap.get(filterAttr);

                if (first) {
                    if (filterAttr.equals(DataNode.this.keyColumn)) {
                        if (filter.operator() == QueryLogic.Filter.Operator.IN) {
                            for (Object value : filter.values()) {
                                final RowCoordinate rowCoord = DataNode.this.rowCoordinateMap.get(value);
                                if (rowCoord != null && rowCoord.batchIndex == batchIndex
                                ) {
                                    if (matches == null) {
                                        matches = new TreeSet<>();
                                    }
                                    matches.add(rowCoord.rowIndex);
                                }
                            }
                        } else {
                            matches = DataNode.this.rowCoordinateMap.entrySet().stream()
                                    .filter(en -> en.getValue().batchIndex == batchIndex)
                                    .filter(en -> !filter.values().contains(en.getKey()))
                                    .map(en -> en.getValue().rowIndex)
                                    .collect(toSet());
                        }
                    } else {
                        for (int rowIndex = 0; rowIndex < vsc.getRowCount(); ++rowIndex) {
                            if (!this.replaced.contains(rowIndex) && filter.match(fv, rowIndex)) {
                                if (matches == null) {
                                    matches = new HashSet<>();
                                }
                                matches.add(rowIndex);
                            }
                        }
                    }

                    first = false;
                } else {
                    if (filterAttr.equals(DataNode.this.keyColumn)) {
                        if (filter.operator() == QueryLogic.Filter.Operator.IN) {
                            for (Object value : filter.values()) {
                                final RowCoordinate rowCoord = DataNode.this.rowCoordinateMap.get(value);
                                if (rowCoord != null &&
                                        rowCoord.batchIndex == batchIndex &&
                                        !this.replaced.contains(rowCoord.rowIndex)
                                ) {
                                    matches.add(rowCoord.rowIndex);
                                }
                            }
                        } else {
                            final Set<Integer> matches2 =
                                    DataNode.this.rowCoordinateMap.entrySet().stream()
                                            .filter(en -> en.getValue().batchIndex == batchIndex)
                                            .filter(en -> !filter.values().contains(en.getKey()))
                                            .map(en -> en.getValue().rowIndex)
                                            .collect(toSet());
                            if (matches2.isEmpty()) {
                                matches.clear();
                            } else {
                                matches.retainAll(matches2);
                            }
                        }
                    } else {
                        Set<Integer> matches2 = null;

                        for (int rowIndex : matches) {
                            if (!this.replaced.contains(rowIndex)) {
                                if (filter.match(fv, rowIndex)) {
                                    if (matches2 == null) {
                                        matches2 = new HashSet<>();
                                    }
                                    matches2.add(rowIndex);
                                }
                            }
                        }

                        if (matches2 != null) {
                            matches = matches2;
                        }
                    }
                }

                if (matches == null || matches.isEmpty()) {
                    break;
                }
            }

            if (matches == null) {
                matches = Collections.emptySet();
            }

            logger.info("Found {} matches", matches.size());

            return matches;
        }
    }

    private final String name;
    private final String keyColumn;
    private final BufferAllocator allocator;
    private final Schema schema;
    private final int keyColumnIndex;
    private final List<Batch> batches;
    private final Map<Object, RowCoordinate> rowCoordinateMap = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public DataNode(
            String name,
            String keyColumn,
            AllocatorManager allocatorManager,
            Schema schema,
            List<ArrowRecordBatch> arbs
    ) {
        this.name = name;
        this.keyColumn = keyColumn;
        this.allocator = allocatorManager.newChildAllocator(name);
        this.schema = schema;
        this.keyColumnIndex = CacheUtils.findKeyColumn(schema, keyColumn);
        this.batches = new ArrayList<>();

        logger.info("Creating new DataNode for name: {} keyName: {}", name, keyColumn);

        addImpl(arbs);
    }

    public DataNode(
            String name,
            String keyColumn,
            AllocatorManager allocatorManager,
            Schema schema
    ) {
        this(
                name,
                keyColumn,
                allocatorManager,
                schema,
                new ArrayList<>()
        );
    }

    public DataNode(
            String name,
            SchemaConfig.TableConfig tableConfig,
            AllocatorManager allocatorManager,
            Schema schema,
            List<ArrowRecordBatch> arbs
    ) {
        this(
                name,
                tableConfig.keyColumn(),
                allocatorManager,
                schema,
                arbs
        );
    }

    public DataNode(
            String name,
            SchemaConfig.TableConfig tableConfig,
            AllocatorManager allocatorManager,
            Schema schema
    ) {
        this(
                name,
                tableConfig,
                allocatorManager,
                schema,
                new ArrayList<>()
        );
    }

    @Override
    public void close() {
        logger.info("Closing DataNode for name: {}", name);
        synchronized (rwLock.writeLock()) {
            for (Batch batch : this.batches) {
                batch.close();
            }
            allocator.close();
        }
    }

    public Schema schema() {
        return schema;
    }

    public synchronized void add(Schema schema, ArrowRecordBatch batch) {
        if (!this.schema.equals(schema)) {
            throw new IllegalArgumentException("Schema mismatch");
        } else {
            addImpl(Collections.singletonList(batch));
        }
    }

    public synchronized void add(Schema schema, List<ArrowRecordBatch> arbs) {
        if (!this.schema.equals(schema)) {
            throw new IllegalArgumentException("Schema mismatch");
        } else {
            addImpl(arbs);
        }
    }

    private void addImpl(List<ArrowRecordBatch> arbs) {
        synchronized (this.rwLock.writeLock()) {
            try (VectorSchemaRoot vsc = VectorSchemaRoot.create(this.schema, this.allocator)) {
                final VectorLoader loader = new VectorLoader(vsc);
                for (final ArrowRecordBatch arb : arbs) {
                    final Batch batch = new Batch(arb);
                    this.batches.add(batch);
                    loader.load(arb);
                    processBatch(this.batches.size() - 1, vsc);
                }
            }
        }
    }

    private void processBatch(int batchIndex, VectorSchemaRoot vsc) {
        final int rowCount =  vsc.getRowCount();
        final FieldVector fv = vsc.getFieldVectors().get(this.keyColumnIndex);
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            final Object key = fv.getObject(rowIndex);
            final RowCoordinate oldRowCoordinate = this.rowCoordinateMap.get(key);
            if (oldRowCoordinate != null) {
                this.batches.get(oldRowCoordinate.batchIndex).markAsReplaced(oldRowCoordinate.rowIndex);
            }
            this.rowCoordinateMap.put(key, new RowCoordinate(batchIndex, rowIndex));
        }
    }

    private boolean isInKeyFilter(QueryLogic.Filter<?> filter) {
        return filter.operator() == QueryLogic.Filter.Operator.IN &&
                filter.attribute().equals(keyColumn);
    }

    public Map<Integer, Set<Integer>> execute(List<Model.Filter<?>> filters) {

        logger.info("Executing query {}", filters);

        filters = TRANSLATE_QUERY.applyFilters(filters);

        logger.info("Translated query {}", filters);

        final QueryLogic queryLogic = new QueryLogic(this.keyColumn, filters);

        logger.info("QueryLogic query {}", queryLogic);

        final Map<Integer, Set<Integer>> batchMatches = new HashMap<>();

        synchronized (this.rwLock.readLock()) {

            try (final VectorSchemaRoot vsc = VectorSchemaRoot.create(this.schema, this.allocator)) {
                List<QueryLogic.Filter<?>> qlFilters = queryLogic.filters();
                if (!qlFilters.isEmpty() && isInKeyFilter(qlFilters.get(0))) {
                    logger.info("Optimising as first filter is an IN filter on the key column");

                    final QueryLogic.Filter<?> inKeyFilter = qlFilters.get(0);

                    inKeyFilter.values().stream()
                            .map(rowCoordinateMap::get)
                            .forEach(rc ->
                                    batchMatches.computeIfAbsent(
                                            rc.batchIndex,
                                            u -> new TreeSet<>()
                                    ).add(rc.rowIndex)
                            );

                    qlFilters = qlFilters.subList(1, qlFilters.size());
                }

                final VectorLoader loader = new VectorLoader(vsc);

                for (int batchIndex = 0; batchIndex < this.batches.size(); ++batchIndex) {
                    final Batch batch = this.batches.get(batchIndex);

                    final Set<Integer> matches = batch.matches(loader, vsc, qlFilters, batchIndex);

                    if (!matches.isEmpty()) {
                        batchMatches.put(batchIndex, matches);
                    }
                }
            }
        }

        return batchMatches;
    }

    public void execute(
            Map<Integer, Set<Integer>> batchMatches,
            FlightProducer.ServerStreamListener listener
    ) {

        synchronized (this.rwLock.readLock()) {
            try (
                    final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(this.schema, this.allocator);
                    final VectorSchemaRoot vsc = VectorSchemaRoot.create(this.schema, this.allocator)
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

                listener.completed();
            }
        }
    }

    public void deleteEntries(Map<Integer, Set<Integer>> batchRows) {
        batchRows.forEach((batchIndex, rowIndexes) -> {
            final DataNode.Batch batch = this.batches.get(batchIndex);
            batch.replaced.addAll(rowIndexes);
        });
    }
}
