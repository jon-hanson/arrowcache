package io.nson.arrowcache.server;

import io.nson.arrowcache.common.Actions;
import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.common.codec.DeleteCodecs;
import io.nson.arrowcache.common.codec.MatchesCodecs;
import io.nson.arrowcache.common.codec.QueryCodecs;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.server.cache.DataNode;
import io.nson.arrowcache.server.cache.DataStore;
import io.nson.arrowcache.server.utils.ArrowServerUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;

public class ArrowCacheProducer extends NoOpFlightProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheProducer.class);

    private static final ActionType DELETE = new ActionType(Actions.DELETE_NAME, "");

    private final Location location;

    private final DataStore dataStore;

    public ArrowCacheProducer(
            DataStore dataStore,
            Location location
    ) {
        this.dataStore = dataStore;
        this.location = location;
    }

    @Override
    public void close() {
    }

    private DataNode getNode(CachePath cachePath) {
        return dataStore.getNode(cachePath)
                .orElseThrow(() ->
                    ArrowServerUtils.notFound(logger, "No data node found for path " + cachePath)
                );
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        logger.info("listActions: {}", context.peerIdentity());
        listener.onNext(DELETE);
        listener.onCompleted();
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        dataStore.getCachePaths()
                .stream()
                .map(path -> FlightDescriptor.path(path.parts()))
                .forEach(flightDesc -> { listener.onNext(getFlightInfo(null, flightDesc)); });
        listener.onCompleted();
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        logger.info("acceptPut: {}", context.peerIdentity());

        final List<ArrowRecordBatch> arbs = new ArrayList<>();

        return () -> {
            logger.info("acceptPut inside runnable");

            long rows = 0;
            while (flightStream.next()) {
                logger.info("Next batch");

                final VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                final ArrowRecordBatch arb = unloader.getRecordBatch();

                logger.info("ArrowRecordBatch: {}", arb);
                arbs.add(arb);

                rows += flightStream.getRoot().getRowCount();
            }

            logger.info("Received {} rows", rows);
            final FlightDescriptor flightDesc = flightStream.getDescriptor();
            if (flightDesc.isCommand()) {
                throw ArrowServerUtils.invalidArgument(
                        logger,
                        "Cannot accept a put operation where the Flight Descriptor is a command - must be a path"
                );
            } else {
                final List<String> flightPath = flightDesc.getPath();
                final CachePath cachePath = CachePath.valueOf(flightPath);
                final Schema schema = flightStream.getSchema();
                dataStore.add(cachePath, schema, arbs);
                ackStream.onCompleted();
            }

            logger.info("acceptPut exiting runnable");
        };
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            if (descriptor.isCommand()) {
                final Api.Query query = QueryCodecs.API_TO_BYTES.decode(descriptor.getCommand());
                final CachePath cachePath = query.path();
                final DataNode dataNode = getNode(cachePath);
                final Map<Integer, Set<Integer>> batchMatches = dataNode.execute(query.filters());
                final byte[] response = MatchesCodecs.API_TO_BYTES.encode(new Api.BatchMatches(cachePath.path(), batchMatches));
                final int numRecords = batchMatches.values().stream().mapToInt(Set::size).sum();
                final FlightEndpoint flightEndpoint = new FlightEndpoint(
                        new Ticket(response),
                        location
                );

                return new FlightInfo(
                        dataNode.schema(),
                        descriptor,
                        Collections.singletonList(flightEndpoint),
                        /*bytes=*/-1,
                        numRecords
                );
            } else {
                throw ArrowServerUtils.invalidArgument(logger, "FlightDescriptors with a path are not supported");
            }
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.internal(logger, "Unexpected exception", ex);
        }
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            logger.info("getStream: {}", context.peerIdentity());

            final ByteArrayInputStream bais = new ByteArrayInputStream(ticket.getBytes());
            final Api.BatchMatches batchMatches = MatchesCodecs.API_TO_STREAM.decode(bais);
            final CachePath cachePath = CachePath.valueOf(batchMatches.path());
            final DataNode dataNode = getNode(cachePath);

            dataNode.execute(batchMatches.matches(), listener);
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.internal(logger, "Unexpected exception", ex);
        }
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

        logger.info(
                "doAction called - CallContext: {}, Action:{} ",
                ArrowUtils.toString(context),
                ArrowUtils.toString(action)
        );

        try {
            if (action.getType().equals(Actions.DELETE_NAME)) {
                final Api.Delete delete = DeleteCodecs.API_TO_BYTES.decode(action.getBody());
                if (!delete.paths().isEmpty()) {
                    for (String path : delete.paths()) {
                        final CachePath cachePath = CachePath.valueOf(path);
                        if (dataStore.deleteNode(cachePath)) {
                            listener.onNext(ArrowUtils.stringToResult("Path '" + path + "' successfully deleted"));
                        } else {
                            listener.onNext(ArrowUtils.stringToResult("WARNING: No data node found for path '" + path + "'"));
                        }
                    }
                }

                if (delete.query().isPresent()) {

                }
            } else {
                logger.error("Action '" + action.getType() + "' not supported");
                throw CallStatus.INVALID_ARGUMENT
                        .withDescription("Action '" + action.getType() + "' not supported")
                        .toRuntimeException();
            }

            listener.onCompleted();
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.internal(logger, "Unexpected exception", ex);
        }
    }
}
