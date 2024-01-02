package io.nson.arrowcache.server;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class ArrowCacheProducer extends NoOpFlightProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheProducer.class);

    private final BufferAllocator allocator;
    private final Location location;

    private final ConcurrentMap<FlightDescriptor, Dataset> datasets;

    public ArrowCacheProducer(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;
        this.datasets = new ConcurrentHashMap<>();
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        logger.info("acceptPut: " + context.peerIdentity());

        final List<ArrowRecordBatch> batches = new ArrayList<>();

        return () -> {
            logger.info("acceptPut inside runnable");

            long rows = 0;
            while (flightStream.next()) {
                logger.info("Next batch");

                final VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                final ArrowRecordBatch arb = unloader.getRecordBatch();
                logger.info("ArrowRecordBatch: " + arb);
                batches.add(arb);

                rows += flightStream.getRoot().getRowCount();
            }

            logger.info("Received " + rows + " rows");

            final Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
            datasets.put(flightStream.getDescriptor(), dataset);
            ackStream.onCompleted();

            logger.info("acceptPut exiting runnable");
        };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        logger.info("getStream: " + context.peerIdentity());

        final FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(ticket.getBytes(), StandardCharsets.UTF_8)
        );

        logger.info("FlightDescriptor: " + flightDescriptor);

        final Dataset dataset = this.datasets.get(flightDescriptor);
        if (dataset == null) {
            throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(
                this.datasets.get(flightDescriptor).getSchema(), allocator)
        ) {
            logger.info("VectorSchemaRoot: " + root.getFieldVectors());

            final VectorLoader loader = new VectorLoader(root);
            listener.start(root);

            for (ArrowRecordBatch arb : this.datasets.get(flightDescriptor).getBatches()) {
                logger.info("ArrowRecordBatch: " + arb);
                loader.load(arb);
                listener.putNext();
            }

            listener.completed();

            logger.info("Done");
        }
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        final FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(action.getBody(), StandardCharsets.UTF_8)
        );

        switch (action.getType()) {
            case "DELETE": {
                final Dataset removed = datasets.remove(flightDescriptor);
                if (removed != null) {
                    try {
                        removed.close();
                    } catch (Exception e) {
                        listener.onError(CallStatus.INTERNAL
                                .withDescription(e.toString())
                                .toRuntimeException());
                        return;
                    }

                    final Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));

                    listener.onNext(result);
                } else {
                    final Result result = new Result("Delete not completed. Reason: Key did not exist."
                            .getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                }

                listener.onCompleted();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        final FlightEndpoint flightEndpoint = new FlightEndpoint(
                new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)),
                location
        );

        return new FlightInfo(
                datasets.get(descriptor).getSchema(),
                descriptor,
                Collections.singletonList(flightEndpoint),
                /*bytes=*/-1,
                datasets.get(descriptor).getRows()
        );
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
        listener.onCompleted();
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(datasets.values());
    }
}
