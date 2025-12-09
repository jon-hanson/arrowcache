package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.server.utils.TranslateStrings;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiPredicate;

import static java.util.stream.Collectors.toList;
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

    private static int findKeyColumn(Schema schema, String name) {
        final List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i).getName().equals(name)) {
                return i;
            }
        }

        throw new RuntimeException("Key column name '" + name + "' not found in schema");
    }

    private final BufferAllocator allocator;
    private final Schema schema;
    private final String keyName;
    private final int keyIndex;
    private final List<ArrowRecordBatch> batches;
    private final List<Set<Integer>> batchReplacedSet = new ArrayList<>();
    private final Map<Comparable<?>, RowCoordinate> rowCoordinateMap = new HashMap<>();

    public DataNode(
            BufferAllocator allocator,
            Schema schema,
            String keyName,
            List<ArrowRecordBatch> batches
    ) {
        this.allocator = allocator;
        this.schema = schema;
        this.keyName = keyName;
        this.keyIndex = findKeyColumn(schema, keyName);
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
        add(Collections.singletonList(batches));
    }

    public synchronized void add(List<ArrowRecordBatch> batches) {
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
        final Api.Query query2 = TranslateStrings.applyQuery(query);

        try (
                VectorSchemaRoot vsc = VectorSchemaRoot.create(
                        schema,
                        allocator
                )
        ) {
            logger.info("VectorSchemaRoot: {}", vsc.getFieldVectors());

            final VectorLoader loader = new VectorLoader(vsc);

            final List<Api.MVFilter<?>> filters = normaliseFilters(query2.filters());

            final List<Set<Integer>> batchMatches =
                    batches.stream()
                            .map(b -> new HashSet<Integer>())
                            .collect(toList());

            final Optional<Api.MVFilter<?>> keyFilter =
                    filters.stream()
                            .filter(f -> f.attribute().equals(keyName))
                            .findAny();

            boolean first = true;

            if (keyFilter.map(Api.MVFilter::operator).map(Api.MVFilter.Operator.IN::equals).orElse(false)) {
                final Api.MVFilter<? extends Comparable<?>> filter = (Api.MVFilter)keyFilter.get();

                for (Comparable<?> value : filter.values()) {
                    final RowCoordinate matchRC = rowCoordinateMap.get(value);
                    if (matchRC != null) {
                        if (first) {
                            first = false;
                        }
                        setMatch(batchMatches, matchRC);
                    }
                }
            }

            for (final Api.MVFilter<?> filter : filters) {
                final String attrName = filter.attribute();

                if (first) {
                    for (int batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {
                        final ArrowRecordBatch batch = batches.get(batchIndex);
                        loader.load(batch);

                        final Set<Integer> replaced = batchReplacedSet.get(batchIndex);

                        final Map<String, FieldVector> fvMap =
                                vsc.getFieldVectors().stream()
                                        .collect(toMap(
                                                fv -> fv.getField().getName(),
                                                fv -> fv
                                        ));

                        final FieldVector fv = fvMap.get(attrName);

                        Set<Integer> matches = batchMatches.get(batchIndex);

                        for (int rowIndex = 0; rowIndex < vsc.getRowCount(); ++rowIndex) {
                            if (!replaced.contains(rowIndex)) {
                                if (filter.alg(FV_FILTER_ALG).test(fv, rowIndex)) {
                                    if (matches == null) {
                                        matches = new HashSet<>();
                                        batchMatches.set(batchIndex, matches);
                                    }
                                    matches.add(rowIndex);
                                }
                            }
                        }
                    }

                    first = false;
                } else {
                    for (int batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {
                        final ArrowRecordBatch batch = batches.get(batchIndex);
                        loader.load(batch);

                        final Set<Integer> replaced = batchReplacedSet.get(batchIndex);

                        final Map<String, FieldVector> fvMap =
                                vsc.getFieldVectors().stream()
                                        .collect(toMap(
                                                fv -> fv.getField().getName(),
                                                fv -> fv
                                        ));

                        final FieldVector fv = fvMap.get(attrName);

                        Set<Integer> matches = batchMatches.get(batchIndex);
                        Set<Integer> matches2 = new TreeSet<>();
                        boolean changed = false;
                        for (int rowIndex : matches) {
                            if (!replaced.contains(rowIndex)) {
                                if (filter.alg(FV_FILTER_ALG).test(fv, rowIndex)) {
                                    matches2.add(rowIndex);
                                    changed = true;
                                }
                            }
                        }

                        if (changed) {
                            batchMatches.set(batchIndex, matches2);;
                        }
                    }
                }
            }

            try (final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(vsc.getSchema(), allocator)) {
                listener.start(resultVsc);

                for (int batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {
                    final ArrowRecordBatch batch = batches.get(batchIndex);
                    final Set<Integer> matches = batchMatches.get(batchIndex);

                    loader.load(batch);

                    VectorSchemaRoot[] slices = null;
                    try {
                        slices = matches.stream()
                                .map(i -> vsc.slice(i, 1))
                                .toArray(VectorSchemaRoot[]::new);

                        resultVsc.allocateNew();
                        VectorSchemaRootAppender.append(false, resultVsc, slices);
                        listener.putNext();
                    } finally {
                        for (VectorSchemaRoot slice : slices) {
                            slice.close();
                        }
                    }
                }

                listener.completed();
            }
        }
    }

    private static void setMatch(List<Set<Integer>> batchMatches, RowCoordinate matchRC) {
        Set<Integer> matches = batchMatches.get(matchRC.batchIndex);
        if (matches == null) {
            matches = new HashSet<>();
            batchMatches.set(matchRC.batchIndex, matches);
        }
        matches.add(matchRC.rowIndex);
    }

    private static final Api.Filter.Alg<BiPredicate<FieldVector, Integer>> FV_FILTER_ALG = new Api.Filter.Alg<>() {
        @Override
        public BiPredicate<FieldVector, Integer> svFilter(String attribute, Api.SVFilter.Operator op, Object value) {
            return (fv, i) -> {
                final Object fvVal = fv.getObject(i);
                final boolean match = value.equals(fvVal);
                switch (op) {
                    case EQUALS:
                        return match;
                    case NOT_EQUALS:
                        return !match;
                    default:
                        throw new IllegalStateException("Unknown filter operator: " + op);
                }
            };
        }

        @Override
        public BiPredicate<FieldVector, Integer> mvFilter(String attribute, Api.MVFilter.Operator op, Set<?> values) {
            return (fv, i) -> {
                final Object fvVal = fv.getObject(i);
                final boolean match = values.contains(fvVal);
                switch (op) {
                    case IN:
                        return match;
                    case NOT_IN:
                        return !match;
                    default:
                        throw new IllegalStateException("Unknown filter operator: " + op);
                }
            };
        }
    };

    private static final class Values<T> {
        Set<T> inclusions = null;
        Set<T> exclusions = null;

        private Set<T> inclusions() {
            if (inclusions == null) {
                inclusions = new HashSet<>();
            }
            return inclusions;
        }

        private Set<T> exclusions() {
            if (exclusions == null) {
                exclusions = new HashSet<>();
            }
            return exclusions;
        }

        void addInclusion(Object value) {
            inclusions().add((T)value);
        }

        void addInclusions(Collection<?> values) {
            inclusions().addAll((Collection)values);
        }

        void addExclusion(Object value) {
            exclusions().add((T)value);
        }

        void addExclusions(Collection<?> values) {
            exclusions().addAll((Collection)values);
        }
    }

    private List<Api.MVFilter<?>> normaliseFilters(Collection<Api.Filter<?>> filters) {
        final Map<String, Values<?>> mapValues = new HashMap<>();
        for (Api.Filter<?> filter : filters) {
            final Values<?> values = mapValues.computeIfAbsent(filter.attribute(), k -> new Values<>());
            if (filter instanceof Api.SVFilter) {
                final Api.SVFilter<?> svFilter = (Api.SVFilter)filter;
                switch (svFilter.operator()) {
                    case EQUALS:
                        values.addInclusion(svFilter.value());
                        break;
                    case NOT_EQUALS:
                        values.addExclusion(svFilter.value());
                        break;
                    default:
                        throw new IllegalStateException("Unknown filter operator: " + svFilter.operator());
                }
            } else if (filter instanceof Api.MVFilter) {
                final Api.MVFilter<?> mvFilter = (Api.MVFilter)filter;
                switch (mvFilter.operator()) {
                    case IN:
                        values.addInclusions(mvFilter.values());
                        break;
                    case NOT_IN:
                        values.addExclusions(mvFilter.values());
                        break;
                    default:
                        throw new IllegalStateException("Unknown filter operator: " + mvFilter.operator());
                }
            }
        }

        final List<Api.MVFilter<?>> groupFilters = new ArrayList<>(mapValues.size());
        mapValues.forEach((name, values) -> {
            if (values.inclusions != null) {
                final Set<?> inclusions = values.inclusions;
                if (values.exclusions != null) {
                    inclusions.removeAll(values.exclusions);
                }
                groupFilters.add(Api.Filter.in(name, inclusions));
            } else if (values.exclusions != null) {
                groupFilters.add(Api.Filter.notIn(name, values.exclusions));
            }
        });

        groupFilters.sort((lhs, rhs) -> {
            final boolean lhsIsKey = lhs.attribute().equals(keyName);
            final boolean rhsIsKey = rhs.attribute().equals(keyName);

            if (lhsIsKey && !rhsIsKey) {
                return -1;
            } else if (rhsIsKey && !lhsIsKey) {
                return 1;
            } else {
                final boolean lhsIsIn = lhs.operator() == Api.MVFilter.Operator.IN;
                final boolean rhsIsIn = rhs.operator() == Api.MVFilter.Operator.IN;

                if (lhsIsIn && !rhsIsIn) {
                    return -1;
                } else if (rhsIsIn && !lhsIsIn) {
                    return 1;
                } else {
                    return lhs.attribute().compareTo(rhs.attribute());
                }
            }
        });

        return groupFilters;
    }

}
