package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.server.QueryLogic;
import io.nson.arrowcache.server.utils.TranslateStrings;
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

public class DataNode implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

    private static class RowCoordinate {
        final int batchIndex;
        final int rowIndex;

        private RowCoordinate(int batchIndex, int rowIndex) {
            this.batchIndex = batchIndex;
            this.rowIndex = rowIndex;
        }
    }

    private final BufferAllocator allocator;
    private final Schema schema;
    private final String keyName;
    private final int keyIndex;
    private final List<ArrowRecordBatch> batches;
    private final List<Set<Integer>> batchReplacedSet = new ArrayList<>();
    private final Map<Comparable<?>, RowCoordinate> rowCoordinateMap = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public DataNode(
            BufferAllocator allocator,
            Schema schema,
            String keyName,
            List<ArrowRecordBatch> batches
    ) {
        this.allocator = allocator;
        this.schema = schema;
        this.keyName = keyName;
        this.keyIndex = CacheUtils.findKeyColumn(schema, keyName);
        this.batches = batches;
    }

    public DataNode(BufferAllocator allocator, Schema schema, String keyName) {
        this(allocator, schema, keyName, new ArrayList<>());
    }

    @Override
    public void close() {
        for (ArrowRecordBatch batch : batches) {
            batch.close();
        }
    }

    public synchronized void add(ArrowRecordBatch batches) {
        synchronized (rwLock.writeLock()) {
            add(Collections.singletonList(batches));
        }
    }

    public synchronized void add(List<ArrowRecordBatch> batches) {
        synchronized (rwLock.writeLock()) {
            try (
                    VectorSchemaRoot vsc = VectorSchemaRoot.create(
                            schema,
                            allocator
                    )
            ) {
                final VectorLoader loader = new VectorLoader(vsc);
                for (int batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {
                    final ArrowRecordBatch batch = batches.get(batchIndex);
                    this.batches.add(batch);
                    this.batchReplacedSet.add(new HashSet<>());
                    loader.load(batch);
                    processBatch(this.batches.size() - 1, vsc);
                }
            }
        }
    }

    private void processBatch(int batchIndex, VectorSchemaRoot vsc) {
        final int rowCount =  vsc.getRowCount();
        final FieldVector fv = vsc.getFieldVectors().get(keyIndex);
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            final String key = fv.getObject(rowIndex).toString();
            final RowCoordinate oldRowCoordinate = rowCoordinateMap.get(key);
            if (oldRowCoordinate != null) {
                batchReplacedSet.get(oldRowCoordinate.batchIndex).add(oldRowCoordinate.rowIndex);
            }
            rowCoordinateMap.put(key, new RowCoordinate(batchIndex, rowIndex));
        }
    }

    public void execute(
            Api.Query query,
            FlightProducer.ServerStreamListener listener
    ) {
        query = TranslateStrings.applyQuery(query);

        final QueryLogic queryLogic = new QueryLogic(keyName, query);

        synchronized (rwLock.readLock()) {
            try (
                    final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(schema, allocator);
                    final VectorSchemaRoot vsc = VectorSchemaRoot.create(schema, allocator)
            ) {
                final VectorLoader loader = new VectorLoader(vsc);

                listener.start(resultVsc);

                for (int batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {

                    final ArrowRecordBatch batch = batches.get(batchIndex);
                    loader.load(batch);
                    final Map<String, FieldVector> fvMap =
                            vsc.getFieldVectors().stream()
                                    .collect(toMap(
                                            fv -> fv.getField().getName(),
                                            fv -> fv
                                    ));
                    final Set<Integer> replaced = batchReplacedSet.get(batchIndex);

                    Set<Integer> matches = null;

                    boolean first = true;

                    for (QueryLogic.Filter<?> filter : queryLogic) {
                        final String filterAttr = filter.attribute();
                        final FieldVector fv = fvMap.get(filterAttr);

                        if (first) {
                            if (filterAttr.equals(keyName) && filter.operator() == QueryLogic.Filter.Operator.IN) {
                                for (Object value : filter.values()) {
                                    final RowCoordinate rowCoord = rowCoordinateMap.get(value);
                                    if (rowCoord != null &&
                                            rowCoord.batchIndex == batchIndex &&
                                            !replaced.contains(rowCoord.rowIndex)
                                    ) {
                                        if (matches == null) {
                                            matches = new HashSet<>();
                                        }
                                        matches.add(rowCoord.rowIndex);
                                    }
                                }
                                matches = new HashSet<>();
                            } else {
                                for (int rowIndex = 0; rowIndex < vsc.getRowCount(); ++rowIndex) {
                                    if (!replaced.contains(rowIndex) && filter.match(fv, rowIndex)) {
                                        if (matches == null) {
                                            matches = new HashSet<>();
                                        }
                                        matches.add(rowIndex);
                                    }
                                }
                            }

                            first = false;
                        } else {
                            if (matches != null && !matches.isEmpty()) {
                                if (filterAttr.equals(keyName) && filter.operator() == QueryLogic.Filter.Operator.IN) {
                                    for (Object value : filter.values()) {
                                        final RowCoordinate rowCoord = rowCoordinateMap.get(value);
                                        if (rowCoord != null &&
                                                rowCoord.batchIndex == batchIndex &&
                                                !replaced.contains(rowCoord.rowIndex)
                                        ) {
                                            matches.add(rowCoord.rowIndex);
                                        }
                                    }
                                    matches = new HashSet<>();
                                } else {
                                    Set<Integer> matches2 = null;

                                    for (int rowIndex : matches) {
                                        if (!replaced.contains(rowIndex)) {
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
                        }
                    }


                    if (matches != null && !matches.isEmpty()) {
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
