package io.nson.arrowcache.client.impl;

import io.nson.arrowcache.client.ClientAPI;
import io.nson.arrowcache.common.Actions;
import io.nson.arrowcache.common.avro.DeleteRequest;
import io.nson.arrowcache.common.avro.FlightInfoRequest;
import io.nson.arrowcache.common.avro.GetRequest;
import io.nson.arrowcache.common.avro.MergeRequest;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.common.utils.ExceptionUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@NullMarked
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

    private static FlightDescriptor flightDescriptor(List<String> schemaPath, String table) {
        final List<String> pathTable = new ArrayList<>(schemaPath.size() + 1);
        pathTable.addAll(schemaPath);
        pathTable.add(table);
        return FlightDescriptor.path(pathTable.toArray(new String[0]));
    }

    @Override
    public void put(List<String> schemaPath, String table, VectorSchemaRoot vsc, Source src) {
        try {
            final FlightDescriptor flightDesc = flightDescriptor(schemaPath, table);

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
    public void put(List<String> schemaPath, String table, VectorSchemaRoot vsc) {
        put(schemaPath, table, vsc, new SingleValueSource());
    }

    @Override
    public void get(List<String> schemaPath, String table, Set<?> keys, Listener listener) {
        try {
            final GetRequest getRequest =
                    GetRequest.newBuilder()
                            .setSchemaPath(schemaPath)
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
    public void remove(List<String> schemaPath, String table, Set<?> keys) {
        try {
            final DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                    .setSchemaPath(schemaPath)
                    .setTable(table)
                    .setKeys(new ArrayList<>(keys))
                    .build();
            final byte[] bytes = DeleteRequest.getEncoder().encode(deleteRequest).array();
            final Action action = Actions.DELETE.createAction(bytes);
            final Iterator<Result> resultIter = flightClient.doAction(action);

            while (resultIter.hasNext()) {
                final Result result = resultIter.next();
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

    @Override
    public void mergeTables(List<String> schemaPath) {
        mergeTablesImpl(schemaPath, Collections.emptySet());
    }

    @Override
    public void mergeTables(List<String> schemaPath, Set<String> tables) {
        if (tables.isEmpty()) {
            throw new RuntimeException("Merge called for an empty set of tables");
        } else {
            mergeTablesImpl(schemaPath, tables);
        }
    }

    private void mergeTablesImpl(List<String> schemaPath, Set<String> tables) {
        try {
            final MergeRequest mergeRequest = MergeRequest.newBuilder()
                    .setSchemaPath(schemaPath)
                    .setTables(new ArrayList<>(tables))
                    .build();
            final byte[] bytes = MergeRequest.getEncoder().encode(mergeRequest).array();
            final Action action = Actions.MERGE.createAction(bytes);
            final Iterator<Result> resultIter = flightClient.doAction(action);

            while (resultIter.hasNext()) {
                final Result result = resultIter.next();
                final String msg = ArrowUtils.resultToString(result);
                logger.info("Merge action result: {}", msg);
            }
        } catch (Exception ex) {
            throw ExceptionUtils.exception(
                    logger,
                    "Exception occurred"
            ).cause(ex).create();
        }
    }
}
