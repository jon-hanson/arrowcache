package io.nson.arrowcache.server;

import io.nson.arrowcache.common.avro.*;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.server.cache.DataSchema;
import io.nson.arrowcache.server.cache.DataTable;
import io.nson.arrowcache.server.utils.ArrowServerUtils;
import io.nson.arrowcache.server.utils.ByteUtils;
import io.nson.arrowcache.server.utils.ExceptionUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toSet;

public class ArrowCacheProducer extends NoOpFlightProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheProducer.class);

    private static final ActionType DELETE = new ActionType("DELETE", "");

    private final SchemaConfig schemaConfig;

    private final Location location;

    private final Duration requestLifetime;

    private final Map<String, DataSchema> dataSchemaMap;

    private final Map<UUID, RequestExecutor> pendingRequests;

    private volatile boolean closing = false;

    public ArrowCacheProducer(
            SchemaConfig schemaConfig,
            Location location,
            Duration requestLifetime,
            Map<String, DataSchema> dataSchemaMap
    ) {
        this.schemaConfig = schemaConfig;
        this.location = location;
        this.requestLifetime = requestLifetime;
        this.dataSchemaMap = dataSchemaMap;
        this.pendingRequests = new ConcurrentHashMap<>();
    }

    public ArrowCacheProducer(
            SchemaConfig schemaConfig,
            Location location,
            Duration requestLifetime
    ) {
        this(schemaConfig, location, requestLifetime, new ConcurrentHashMap<>());
    }

    @Override
    public void close() {
        logger.info("Closing...");
        closing = true;
        dataSchemaMap.values().forEach(DataSchema::close);
    }

    private void closeStaleRequests() {
        if (!closing) {
            final Instant cutoff = Instant.now().minus(requestLifetime);
            final Set<UUID> keysToRetire =
                    pendingRequests.values().stream()
                            .filter(req -> req.inception().isBefore(cutoff))
                            .map(RequestExecutor::uuid)
                            .collect(toSet());
            keysToRetire.forEach(pendingRequests::remove);
            keysToRetire.stream()
                    .map(pendingRequests::get)
                    .forEach(RequestExecutor::close);
        }
    }

    private DataSchema getDataSchema(String schemaName) {
        return Optional.ofNullable(dataSchemaMap.get(schemaName))
                .orElseThrow(() ->
                        ArrowServerUtils.logError(
                                CallStatus.NOT_FOUND,
                                        logger,
                                        "No schema with name: " + schemaName
                                ).toRuntimeException()
                );
    }

    private DataTable getDataTable(DataSchema dataSchema, String tableName) {
        return dataSchema.getTableOpt(tableName)
                .orElseThrow(() ->
                        ArrowServerUtils.logError(
                                CallStatus.NOT_FOUND,
                                logger,
                                "No table in Schema '" + dataSchema.name() + "' with name: " + tableName
                        ).toRuntimeException()
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
        try {
            final CriteriaRequest criteriaReq = CriteriaRequest.getDecoder().decode(criteria.getExpression());

            for (String schemaName : criteriaReq.getSchemas()) {
                final DataSchema schema = getDataSchema(schemaName);

                final Map<String, DataTable> tableMap = schema.getTableMap();

                final FlightEndpoint flightEndpoint = new FlightEndpoint(
                        new Ticket(new byte[]{}),
                        location
                );

                tableMap.forEach((tableName, dataTable) -> {
                    final FlightDescriptor descriptor = FlightDescriptor.path(schema.name(), tableName);
                    final FlightInfo flightInfo = new FlightInfo(
                            dataTable.arrowSchema().get(),
                            descriptor,
                            Collections.singletonList(flightEndpoint),
                            -1,
                            -1
                    );
                    listener.onNext(flightInfo);
                });
            }

            listener.onCompleted();
        } catch (IOException ex) {
            logger.error("Failure in listFlights()", ex);
            listener.onError(ex);
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            if (descriptor.isCommand()) {
                logger.info(
                        "getFlightInfo - context: {} descriptor: {}",
                        ArrowUtils.toString(context),
                        ArrowUtils.toString(descriptor)
                );

                final FlightInfoRequest flightInfoRequest = FlightInfoRequest.getDecoder().decode(descriptor.getCommand());
                final Object request = flightInfoRequest.getRequest();

                final RequestExecutor requestExecutor;

                if (request instanceof GetRequest) {
                    final GetRequest getRequest = (GetRequest) request;
                    final DataSchema dataSchema = getDataSchema(getRequest.getSchema$());
                    final DataTable dataTable = getDataTable(dataSchema, getRequest.getTable());
                    final List<Object> keys = getRequest.getKeys();
                    logger.info("FlightDescriptor GetRequest: {} keys", keys.size());
                    requestExecutor = RequestExecutor.getRequestExecutor(location, dataTable, keys);
                } else if (request instanceof QueryRequest) {
                    final QueryRequest  queryRequest = (QueryRequest) request;
                    final String sql = queryRequest.getSql();

                    logger.info("FlightDescriptor QueryRequest: {}", sql);

                    requestExecutor = RequestExecutor.queryRequestExecutor(sql);
                } else {
                    throw new IllegalArgumentException("Unsupported request type: " + request.getClass());
                }

                pendingRequests.put(requestExecutor.uuid(), requestExecutor);
                return requestExecutor.getFlightInfo(descriptor);
            } else {
                throw ArrowServerUtils.logError(
                        CallStatus.INVALID_ARGUMENT,
                        logger,
                        "Path-based FlightDescriptors  are not supported"
                ).toRuntimeException();
            }
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.logError(CallStatus.INTERNAL, logger, "Unexpected exception")
                    .withCause(ex)
                    .toRuntimeException();
        }
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            logger.info("getStream: {}", ArrowUtils.toString(context));

            final UUID uuid = ByteUtils.asUuid(ticket.getBytes());
            final RequestExecutor requestExecutor = pendingRequests.get(uuid);
            if (requestExecutor == null) {
                listener.error(
                        ArrowServerUtils.logError(
                                CallStatus.NOT_FOUND,
                                logger,
                                "No pending query found for ticket UUID " + uuid
                        ).toRuntimeException()
                );
            } else {
                requestExecutor.execute(listener);

                listener.completed();
            }
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.logError(CallStatus.INTERNAL, logger, "Unexpected exception")
                    .withCause(ex)
                    .toRuntimeException();
        }
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        logger.info("acceptPut: {}", context.peerIdentity());

        return () -> {
            logger.info("acceptPut inside runnable");

            final Schema arrowSchema = flightStream.getSchema();
            try {
                final FlightDescriptor flightDesc = flightStream.getDescriptor();
                if (flightDesc.isCommand()) {
                    throw ArrowServerUtils.logError(
                            CallStatus.INVALID_ARGUMENT,
                            logger,
                            "Cannot accept a put operation where the FlightDescriptor is a command - must be a path"
                    ).toRuntimeException();
                } else {
                    final List<String> flightPath = flightDesc.getPath();

                    final String schemaName;
                    final String tableName;
                    if (flightPath.size() == 2) {
                        schemaName = flightPath.get(0);
                        tableName = flightPath.get(1);
                    } else {
                        throw ExceptionUtils.logError(
                                logger,
                                Exception::new,
                                "Invalid flight path: " + flightPath + ". Must be [schema, table]"
                        );
                    }

                    final DataSchema dataSchema = getDataSchema(schemaName);
                    final DataTable table = getDataTable(dataSchema, tableName);

                    long rows = 0;
                    while (flightStream.next()) {
                        logger.info("Processing next batch");

                        final VectorUnloader unloader = new VectorUnloader(flightStream.getRoot());
                        final ArrowRecordBatch arb = unloader.getRecordBatch();

                        logger.debug("ArrowRecordBatch: {}", arb.toString());

                        table.addBatch(arrowSchema, arb);

                        rows += flightStream.getRoot().getRowCount();
                    }

                    logger.info("Received {} rows", rows);

                    ackStream.onCompleted();
                }

                logger.info("acceptPut exiting runnable");
            } catch (Exception ex) {
                ackStream.onError(ex);
            }
        };
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

        logger.info(
                "doAction called - CallContext: {}, Action.type:{} ",
                ArrowUtils.toString(context),
                action.getType()
        );

        try {
            if (action.getType().equals(DELETE.getType())) {
                final DeleteRequest deleteRequest = DeleteRequest.getDecoder().decode(action.getBody());
                final DataSchema dataSchema = getDataSchema(deleteRequest.getSchema$());
                final DataTable table = getDataTable(dataSchema, deleteRequest.getTable());

                table.deleteEntries(deleteRequest.getKeys());

                listener.onCompleted();
            } else {
                listener.onError(
                        ArrowServerUtils.logError(
                                CallStatus.INVALID_ARGUMENT,
                                logger,
                                "Action type '" + action.getType() + "' not supported"
                        ).toRuntimeException()
                );
            }
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            listener.onError(
                    ArrowServerUtils.logError(CallStatus.INTERNAL, logger, "Unexpected exception")
                            .withCause(ex)
                            .toRuntimeException()
            );
        }
    }
}
