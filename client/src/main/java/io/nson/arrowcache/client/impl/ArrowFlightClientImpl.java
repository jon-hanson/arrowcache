package io.nson.arrowcache.client.impl;

import io.nson.arrowcache.client.ClientAPI;
import io.nson.arrowcache.common.Actions;
import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.TablePath;
import io.nson.arrowcache.common.codec.DeleteCodecs;
import io.nson.arrowcache.common.codec.QueryCodecs;
import io.nson.arrowcache.common.utils.ArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ArrowFlightClientImpl implements ClientAPI {
    private static final Logger logger = LoggerFactory.getLogger(ArrowFlightClientImpl.class);

    private static final CallOption DEFAULT_CALL_TIMEOUT = CallOptions.timeout(1, TimeUnit.HOURS);

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

    @Override
    public void put(TablePath path, VectorSchemaRoot vsc, Source src) {
        try {
            final FlightDescriptor flightDesc = FlightDescriptor.path(path.parts());

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
    public void put(TablePath path, VectorSchemaRoot vsc) {
        put(path, vsc, new SingleValueSource());
    }

    @Override
    public void get(TablePath path, List<Model.Filter<?>> filters, Listener listener) {
        try {
            final Model.Query query = new Model.Query(path, filters);
            final byte[] bytes = QueryCodecs.MODEL_TO_BYTES.encode(query);
            final FlightDescriptor flightDesc = FlightDescriptor.command(bytes);
            final FlightInfo flightInfo = flightClient.getInfo(flightDesc, callTimeout);

            flightInfo.getEndpoints()
                    .forEach(endPoint -> {
                        for (Location loc : endPoint.getLocations()) {
                            if (!loc.equals(location)) {
                                logger.error("Cannot handle location {}", loc.getUri());
                                throw new RuntimeException("Cannot handle location " + loc.getUri());
                            } else {
                                try (final FlightStream flightStream = flightClient.getStream(endPoint.getTicket(), callTimeout)) {
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
                logger.error("listener.onCompleted() threw exception", ex);
                throw new RuntimeException("listener.onCompleted() error", ex);
            }
        } catch (Exception ex) {
            logger.error("Exception occurred", ex);
            listener.onError(ex);
            throw ex;
        }
    }

    @Override
    public void remove(TablePath path, List<Model.Filter<?>> filters) {
        try {
            final Model.Delete delete = new Model.Delete(path, filters);
            final byte[] bytes = DeleteCodecs.MODEL_TO_BYTES.encode(delete);
            final Action action = new Action(Actions.DELETE_NAME, bytes);
            final Iterator<Result> deleteActionResult = flightClient.doAction(action);

            while (deleteActionResult.hasNext()) {
                final Result result = deleteActionResult.next();
                final String msg = ArrowUtils.resultToString(result);
                logger.info("Delete action result: {}", msg);
            }
        } catch (Exception ex) {
            logger.error("Exception occurred", ex);
            throw ex;
        }
    }
}
