package io.nson.arrowcache.server;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.utils.ByteUtils;
import io.nson.arrowcache.common.QueryCodecs;
import io.nson.arrowcache.server.utils.ArrowUtils;
import io.nson.arrowcache.server.utils.TranslateStrings;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.slf4j.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiPredicate;

import static java.util.stream.Collectors.toMap;

public class ArrowCacheProducer extends NoOpFlightProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheProducer.class);

    private static final String DO_GET_NAME = "DoGet";
    private static final ActionType DO_GET = new ActionType(DO_GET_NAME, "");

    private static final String DO_PUT_NAME = "DoPut";
    private static final ActionType DO_PUT = new ActionType(DO_PUT_NAME, "");

    private final BufferAllocator allocator;
    private final Location location;

    private final ConcurrentMap<FlightDescriptor, Dataset> datasets;

    public ArrowCacheProducer(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;
        this.datasets = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(datasets.values());
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        logger.info("listActions: {}", context.peerIdentity());
        listener.onNext(DO_GET);
        listener.onNext(DO_PUT);
        listener.onCompleted();
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
        listener.onCompleted();
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        logger.info("acceptPut: {}", context.peerIdentity());

        final List<ArrowRecordBatch> batches = new ArrayList<>();

        return () -> {
            logger.info("acceptPut inside runnable");

            long rows = 0;
            while (flightStream.next()) {
                logger.info("Next batch");

                final VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                final ArrowRecordBatch arb = unloader.getRecordBatch();

                logger.info("ArrowRecordBatch: {}", arb);
                batches.add(arb);

                rows += flightStream.getRoot().getRowCount();
            }

            logger.info("Received {} rows", rows);

            final Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
            datasets.put(flightStream.getDescriptor(), dataset);
            ackStream.onCompleted();

            logger.info("acceptPut exiting runnable");
        };
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final long records;
            final Schema schema;
            if (descriptor.isCommand()) {
                final Api.Query query = QueryCodecs.API_TO_BYTES.decode(descriptor.getCommand());

                baos.write(1);
                QueryCodecs.API_TO_STREAM.encode(query).accept(baos);

                records = -1;
                schema = datasets.values().stream().findFirst().map(Dataset::getSchema).orElseThrow();
            } else {
                baos.write(0);

                baos.write(ByteUtils.stringToBytes(descriptor.getPath().get(0)));

                final Dataset dataset = datasets.get(descriptor);
                if (dataset == null) {
                    logger.error("Unknown descriptor");
                    throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
                } else {
                    records = dataset.getRows();
                    schema = dataset.getSchema();
                }
            }

            final FlightEndpoint flightEndpoint = new FlightEndpoint(
                    new Ticket(baos.toByteArray()),
                    location
            );

            return new FlightInfo(
                    schema,
                    descriptor,
                    Collections.singletonList(flightEndpoint),
                    /*bytes=*/-1,
                    records
            );
        } catch (Exception ex) {
            logger.error("Error servicing query", ex);
            throw CallStatus.UNKNOWN.withDescription("Unexpected exception")
                    .withCause(ex)
                    .toRuntimeException();
        }
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            logger.info("getStream: {}", context.peerIdentity());

            final ByteArrayInputStream bais = new ByteArrayInputStream(ticket.getBytes());
            final int flag = bais.read();
            switch (flag) {
                case -1:
                    logger.error("Ticket payload is empty");
                    throw CallStatus.INVALID_ARGUMENT.withDescription("Ticket payload is empty").toRuntimeException();
                case 0:
                    logger.info("Ticket case 0: path");
                    executeGetAll(bais, listener);
                break;
                case 1:
                    logger.info("Ticket case 1: query");
                    executeQuery(bais, listener);
                break;
            }
        } catch (Exception ex) {
            logger.error("Error servicing query", ex);
            throw CallStatus.UNKNOWN.withDescription("Unexpected exception")
                    .withCause(ex)
                    .toRuntimeException();
        }
    }

    private void executeGetAll(ByteArrayInputStream bais, ServerStreamListener listener) {

        final FlightDescriptor flightDescriptor = FlightDescriptor.path(
                ByteUtils.bytesToString(bais.readAllBytes())
        );

        logger.info("FlightDescriptor: {}", flightDescriptor);

        final Dataset dataset = this.datasets.get(flightDescriptor);
        if (dataset == null) {
            logger.error("Unknown descriptor");
            throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
        }

        try (
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        dataset.getSchema(),
                        allocator
                )
        ) {
            logger.info("VectorSchemaRoot: {}", root.getFieldVectors());

            final VectorLoader loader = new VectorLoader(root);
            listener.start(root);

            for (ArrowRecordBatch arb : dataset.getBatches()) {
                logger.info("ArrowRecordBatch: {}", arb);
                loader.load(arb);
                listener.putNext();
            }

            listener.completed();

            logger.info("Done");
        }
    }

    private void executeQuery(ByteArrayInputStream bais, ServerStreamListener listener) {
        final Api.Query query = QueryCodecs.API_TO_STREAM.decode(bais);
        final Api.Query query2 = TranslateStrings.applyQuery(query);
        for (Dataset dataset : this.datasets.values()) {
            for (ArrowRecordBatch arb : dataset.getBatches()) {
                try (
                        VectorSchemaRoot vsc = VectorSchemaRoot.create(
                                dataset.getSchema(),
                                allocator
                        )
                ) {
                    logger.info("VectorSchemaRoot: {}", vsc.getFieldVectors());

                    final VectorLoader loader = new VectorLoader(vsc);
                    loader.load(arb);

                    final Map<String, FieldVector> fvMap =
                            vsc.getFieldVectors().stream()
                                    .collect(toMap(
                                            fv -> fv.getField().getName(),
                                            fv -> fv
                                    ));

                    Set<Integer> matches = null;
                    for (Api.Filter<?> filter : query2.filters()) {
                        final String attrName = filter.attribute();
                        final FieldVector fv = fvMap.get(attrName);
                        if (matches == null) {
                            matches = new TreeSet<>();
                            for (int i = 0; i < dataset.getRows(); ++i) {
                                if (filter.alg(ARROW_FILTER_ALG).test(fv, i)) {
                                    matches.add(i);
                                }
                            }
                        } else {
                            Set<Integer> matches2 = new TreeSet<>();
                            for (int i : matches) {
                                if (filter.alg(ARROW_FILTER_ALG).test(fv, i)) {
                                    matches2.add(i);
                                }
                            }

                            matches = matches2;
                        }

                        if (matches.isEmpty()) {
                            break;
                        }
                    }

                    final int numRecords = matches == null ? 0 : matches.size();

                    logger.info("Found {} matches", numRecords);

                    try (final VectorSchemaRoot resultVsc = VectorSchemaRoot.create(vsc.getSchema(), allocator)) {
                        VectorSchemaRoot[] slices = null;
                        try {
                            slices = matches.stream()
                                    .map(i -> vsc.slice(i, 1))
                                    .toArray(VectorSchemaRoot[]::new);

                            resultVsc.allocateNew();
                            listener.start(resultVsc);
                            VectorSchemaRootAppender.append(false, resultVsc, slices);
                            listener.putNext();
                            listener.completed();
                        } finally {
                            for (VectorSchemaRoot slice : slices) {
                                slice.close();
                            }
                        }
                    }
                }
            }
        }
    }

    private static final Api.Filter.Alg<BiPredicate<FieldVector, Integer>> ARROW_FILTER_ALG = new Api.Filter.Alg<>() {
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

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

        logger.info(
                "doAction called - CallContext: {}, Action:{} ",
                ArrowUtils.toString(context),
                ArrowUtils.toString(action)
        );

        final FlightDescriptor flightDescriptor = FlightDescriptor.path(
                ByteUtils.bytesToString(action.getBody())
        );

        switch (action.getType()) {
            case "DELETE": {
                logger.info("Delete: {}", flightDescriptor);
                final Dataset removed = datasets.remove(flightDescriptor);
                logger.info("    Removed dataset: {}", removed);

                if (removed != null) {
                    try {
                        removed.close();
                    } catch (Exception e) {
                        listener.onError(CallStatus.INTERNAL
                                .withDescription(e.toString())
                                .toRuntimeException());
                        return;
                    }

                    final Result result = ArrowUtils.stringToResult("Delete completed");

                    listener.onNext(result);
                } else {
                    final Result result = ArrowUtils.stringToResult("Delete not completed. Reason: Key did not exist.");
                    listener.onNext(result);
                }

                listener.onCompleted();
            }
        }
    }
}
