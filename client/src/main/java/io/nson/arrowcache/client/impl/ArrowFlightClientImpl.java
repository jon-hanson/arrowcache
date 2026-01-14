package io.nson.arrowcache.client.impl;

import io.nson.arrowcache.client.ClientAPI;
import io.nson.arrowcache.common.avro.DeleteRequest;
import io.nson.arrowcache.common.avro.FlightInfoRequest;
import io.nson.arrowcache.common.avro.GetRequest;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.common.utils.ExceptionUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ArrowFlightClientImpl implements ClientAPI {
    private static final Logger logger = LoggerFactory.getLogger(ArrowFlightClientImpl.class);

    private static final CallOption DEFAULT_CALL_TIMEOUT = CallOptions.timeout(1, TimeUnit.MINUTES);

    public static ClientAPI create(BufferAllocator allocator, Location location, FlightClient flightClient) {
        return new ArrowFlightClientImpl(allocator, location, flightClient, DEFAULT_CALL_TIMEOUT);
    }

    public static ClientAPI create(Location location, FlightClient flightClient) {
        return new ArrowFlightClientImpl(new RootAllocator(), location, flightClient, DEFAULT_CALL_TIMEOUT);
    }

    public static ClientAPI create(Location location) {
        final BufferAllocator allocator = new RootAllocator();
        return new ArrowFlightClientImpl(allocator, location, FlightClient.builder(allocator, location).build(), DEFAULT_CALL_TIMEOUT);
    }

    private final BufferAllocator allocator;
    private final Location location;
    private final FlightClient flightClient;
    private final CallOption callTimeout;

    private ArrowFlightClientImpl(
            BufferAllocator allocator,
            Location location,
            FlightClient flightClient,
            CallOption callTimeout
    ) {
        this.allocator = allocator;
        this.location = location;
        this.flightClient = flightClient;
        this.callTimeout = callTimeout;
    }

    @Override
    public void close() throws Exception {
        flightClient.close();
        allocator.close();
    }

    private static FlightDescriptor flightDescriptor(List<String> path, String table) {
        final List<String> pathTable = new ArrayList<>(path.size() + 1);
        pathTable.addAll(path);
        pathTable.add(table);
        return FlightDescriptor.path(pathTable.toArray(new String[0]));
    }

    @Override
    public void put(List<String> path, String table, VectorSchemaRoot vsc, Source src) {
        try {
            final FlightDescriptor flightDesc = flightDescriptor(path, table);

            final FlightClient.ClientStreamListener listener = flightClient.startPut(
                    flightDesc,
                    vsc,
                    new AsyncPutListener(),
                    callTimeout
            );

            try {
                while (src.hasNext()) {
                    src.loadNext();
                    listener.putNext();
                }

                listener.completed();

                listener.getResult();
            } catch (Exception ex) {
                logger.error("Error while putting into Arrow Flight Client", ex);
                listener.error(ex);
                throw ex;
            }
        } catch (Exception ex) {
            logger.error("Exception occurred", ex);
        }
    }

    private static class SingleValueSource implements Source {

        boolean hasNext = true;

        @Override
        public boolean hasNext() {
            if (hasNext) {
                hasNext = false;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void loadNext() {}
    }

    @Override
    public void put(List<String> path, String table, VectorSchemaRoot vsc) {
        put(path, table, vsc, new SingleValueSource());
    }

    @Override
    public void get(List<String> path, String table, Set<?> keys, Listener listener) {
        try {
            final GetRequest getRequest =
                    GetRequest.newBuilder()
                            .setSchema$(schema)
                            .setTable(table)
                            .setKeys(new ArrayList<>(keys))
                            .build();
            final FlightInfoRequest flightInfoRequest =
                    FlightInfoRequest.newBuilder()
                            .setRequest(getRequest)
                            .build();

            final byte[] bytes = FlightInfoRequest.getEncoder().encode(flightInfoRequest).array();
            final FlightDescriptor flightDesc = FlightDescriptor.command(bytes);
            final FlightInfo flightInfo = flightClient.getInfo(flightDesc, callTimeout);

            flightInfo.getEndpoints()
                    .forEach(endPoint -> {
                        for (Location loc : endPoint.getLocations()) {
                            if (!loc.equals(location)) {
                                throw ExceptionUtils.exception(
                                        logger,
                                        "Cannot handle location " + loc.getUri()
                                ).create();
                            } else {
                                try (final FlightStream flightStream =
                                             flightClient.getStream(endPoint.getTicket(), callTimeout)
                                ) {
                                    final VectorSchemaRoot vsc = flightStream.getRoot();
                                    while (flightStream.next()) {
                                        listener.onNext(vsc);
                                    }
                                } catch (Exception ex) {
                                    listener.onError(ex);
                                    throw new RuntimeException(ex);
                                }
                            }
                        }
                    });

            try {
                listener.onCompleted();
            } catch (Exception ex) {
                throw ExceptionUtils.exception(
                        logger,
                        "listener.onCompleted() threw exception"
                ).create();
            }
        } catch (Exception ex) {
            listener.onError(ex);
            throw ExceptionUtils.exception(logger, "Exception occurred").create();
        }
    }

    @Override
    public void remove(List<String> path, String table, Set<?> keys) {
        try {
            final DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                    .setSchema$(schema)
                    .setTable(table)
                    .setKeys(new ArrayList<>(keys))
                    .build();
            final byte[] bytes = DeleteRequest.getEncoder().encode(deleteRequest).array();
            final Action action = new Action("DELETE", bytes);
            final Iterator<Result> deleteActionResult = flightClient.doAction(action);

            while (deleteActionResult.hasNext()) {
                final Result result = deleteActionResult.next();
                final String msg = ArrowUtils.resultToString(result);
                logger.info("Delete action result: {}", msg);
            }
        } catch (Exception ex) {
            throw ExceptionUtils.exception(
                    logger,
                    "Exception occurred"
            ).cause(ex).create();
        }
    }
}
