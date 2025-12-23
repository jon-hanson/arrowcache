package io.nson.arrowcache.server;

import io.nson.arrowcache.common.Actions;
import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.TablePath;
import io.nson.arrowcache.common.codec.DeleteCodecs;
import io.nson.arrowcache.common.codec.NodeEntrySpecCodecs;
import io.nson.arrowcache.common.codec.QueryCodecs;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.server.cache.DataNode;
import io.nson.arrowcache.server.cache.DataStore;
import io.nson.arrowcache.server.utils.ArrowServerUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        logger.info("Closing...");
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

        return () -> {
            logger.info("acceptPut inside runnable");

            final List<ArrowRecordBatch> arbs = new ArrayList<>();
            try {
                long rows = 0;
                while (flightStream.next()) {
                    logger.info("Next batch");

                    final VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                    final ArrowRecordBatch arb = unloader.getRecordBatch();

                    logger.debug("ArrowRecordBatch: {}", arb);
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
                    final TablePath tablePath = TablePath.valueOfIter(flightPath);
                    final Schema schema = flightStream.getSchema();
                    dataStore.add(tablePath, schema, arbs);
                    arbs.clear();
                    ackStream.onCompleted();
                }

                logger.info("acceptPut exiting runnable");
            } catch (Exception ex) {
                ackStream.onError(ex);
            } finally {
                try {
                    AutoCloseables.close(arbs);
                } catch (Exception ex) {
                    logger.warn("Suppressing exception while closing ArrowRecordBatch", ex);
                }
            }
        };
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            if (descriptor.isCommand()) {
                final Model.Query query = QueryCodecs.MODEL_TO_BYTES.decode(descriptor.getCommand());
                final TablePath tablePath = query.path();
                final DataNode dataNode = dataStore.getNode(tablePath);
                final Map<Integer, Set<Integer>> batchMatches = dataNode.execute(query.filters());
                final byte[] response = NodeEntrySpecCodecs.MODEL_TO_BYTES.encode(new Model.NodeEntrySpec(tablePath.path(), batchMatches));
                final int numRecords = batchMatches.values().stream().mapToInt(Set::size).sum();
                final FlightEndpoint flightEndpoint = new FlightEndpoint(new Ticket(response), location);

                return new FlightInfo(
                        dataNode.schema(),
                        descriptor,
                        Collections.singletonList(flightEndpoint),
                        /*bytes=*/-1,
                        numRecords
                );
            } else {
                throw ArrowServerUtils.invalidArgument(logger, "Path-based FlightDescriptors  are not supported");
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

            final Model.NodeEntrySpec nodeEntrySpec = NodeEntrySpecCodecs.MODEL_TO_BYTES.decode(ticket.getBytes());
            final TablePath tablePath = TablePath.valueOfConcat(nodeEntrySpec.path());
            final DataNode dataNode = dataStore.getNode(tablePath);

            dataNode.execute(nodeEntrySpec.batchRows(), listener);
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.internal(logger, "Unexpected exception", ex);
        }
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

        logger.info(
                "doAction called - CallContext: {}, Action.type:{} ",
                ArrowUtils.toString(context),
                action.getType()
        );

        try {
            if (action.getType().equals(Actions.DELETE_NAME)) {
                final Model.Delete delete = DeleteCodecs.MODEL_TO_BYTES.decode(action.getBody());

                final TablePath tablePath = delete.path();

                if (delete.filters().isEmpty()) {
                    if (dataStore.deleteNode(tablePath)) {
                        listener.onNext(ArrowUtils.stringToResult("Path '" + tablePath + "' successfully deleted"));
                    } else {
                        listener.onNext(ArrowUtils.stringToResult("WARNING: No data node found for path '" + tablePath + "'"));
                    }
                } else {
                    dataStore.deleteEntries(tablePath, delete.filters());
                }
            } else {
                throw ArrowServerUtils.invalidArgument(logger, "Action type '" + action.getType() + "' not supported");
            }

            listener.onCompleted();
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.internal(logger, "Unexpected exception", ex);
        }
    }
}
