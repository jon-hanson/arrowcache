package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Api;
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

import static java.util.stream.Collectors.*;

public class DataNode implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

    private static final TranslateQuery TRANSLATE_QUERY = new TranslateQuery(true, true);

    private static class RowCoordinate {
        final int batchIndex;
        final int rowIndex;

        private RowCoordinate(int batchIndex, int rowIndex) {
            this.batchIndex = batchIndex;
            this.rowIndex = rowIndex;
        }
    }

    private class Batch implements AutoCloseable {
        private final ArrowRecordBatch arb;
        private final Set<Integer> replaced;

        private Batch(ArrowRecordBatch arb, Set<Integer> replaced) {
            this.arb = arb;
            this.replaced = replaced;
        }

        private Batch(ArrowRecordBatch batch) {
            this(batch, new HashSet<>());
        }

        @Override
        public void close() {
            arb.close();
        }

        public void markAsReplaced(int rowIndex) {
            replaced.add(rowIndex);
        }

        public Set<Integer> matches(
                VectorLoader loader,
                VectorSchemaRoot vsc,
                QueryLogic queryLogic,
                int batchIndex
        ) {

            loader.load(arb);
            final Map<String, FieldVector> fvMap =
                    vsc.getFieldVectors().stream()
                            .collect(toMap(
                                    fv -> fv.getField().getName(),
                                    fv -> fv
                            ));

            Set<Integer> matches = null;

            boolean first = true;

            for (QueryLogic.Filter<?> filter : queryLogic.filters()) {
                final String filterAttr = filter.attribute();
                final FieldVector fv = fvMap.get(filterAttr);

                if (first) {
                    if (filterAttr.equals(DataNode.this.keyName)) {
                        if (filter.operator() == QueryLogic.Filter.Operator.IN) {
                            for (Object value : filter.values()) {
                                final RowCoordinate rowCoord = DataNode.this.rowCoordinateMap.get(value);
                                if (rowCoord != null &&
                                        rowCoord.batchIndex == batchIndex &&
                                        !this.replaced.contains(rowCoord.rowIndex)
                                ) {
                                    if (matches == null) {
                                        matches = new HashSet<>();
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
                    if (filterAttr.equals(DataNode.this.keyName)) {
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

            return matches == null ? Collections.emptySet() : matches;
        }
    }

    private final BufferAllocator allocator;
    private final Schema schema;
    private final String keyName;
    private final int keyIndex;
    private final List<Batch> batches;
    private final Map<Object, RowCoordinate> rowCoordinateMap = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public DataNode(
            BufferAllocator allocator,
            Schema schema,
            String keyName,
            List<ArrowRecordBatch> arbs
    ) {
        this.allocator = allocator;
        this.schema = schema;
        this.keyName = keyName;
        this.keyIndex = CacheUtils.findKeyColumn(schema, keyName);
        this.batches = arbs.stream().map(Batch::new).collect(toList());
    }

    public DataNode(BufferAllocator allocator, Schema schema, String keyName) {
        this(allocator, schema, keyName, Collections.emptyList());
    }

    @Override
    public void close() {
        for (Batch batch : this.batches) {
            batch.close();
        }
    }

    public synchronized void add(ArrowRecordBatch batch) {
        add(Collections.singletonList(batch));
    }

    public synchronized void add(List<ArrowRecordBatch> arbs) {
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
        final FieldVector fv = vsc.getFieldVectors().get(this.keyIndex);
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            final Object key = fv.getObject(rowIndex);
            final RowCoordinate oldRowCoordinate = this.rowCoordinateMap.get(key);
            if (oldRowCoordinate != null) {
                this.batches.get(oldRowCoordinate.batchIndex).markAsReplaced(oldRowCoordinate.rowIndex);
            }
            this.rowCoordinateMap.put(key, new RowCoordinate(batchIndex, rowIndex));
        }
    }

    public void execute(
            Api.Query query,
            FlightProducer.ServerStreamListener listener
    ) {
        query = TRANSLATE_QUERY.applyQuery(query);

        final QueryLogic queryLogic = new QueryLogic(this.keyName, query);

        synchronized (this.rwLock.readLock()) {
            try (
                    final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(this.schema, this.allocator);
                    final VectorSchemaRoot vsc = VectorSchemaRoot.create(this.schema, this.allocator)
            ) {
                final VectorLoader loader = new VectorLoader(vsc);

                listener.start(resultVsc);

                for (int batchIndex = 0; batchIndex < this.batches.size(); ++batchIndex) {
                    final Batch batch = this.batches.get(batchIndex);

                    final Set<Integer> matches = batch.matches(loader, vsc, queryLogic, batchIndex);

                    if (!matches.isEmpty()) {
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
                    }
                }

                listener.completed();
            }
        }
    }
}
