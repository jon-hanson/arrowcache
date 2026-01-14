package io.nson.arrowcache.server;

import io.nson.arrowcache.common.avro.*;
import io.nson.arrowcache.common.utils.ArrowUtils;
import io.nson.arrowcache.common.utils.ExceptionUtils;
import io.nson.arrowcache.server.cache.DataSchema;
import io.nson.arrowcache.server.cache.DataTable;
import io.nson.arrowcache.server.utils.ArrowServerUtils;
import io.nson.arrowcache.server.utils.ByteUtils;
import io.nson.arrowcache.server.utils.ConcurrencyUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
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
import java.util.concurrent.ConcurrentMap;

import static java.util.stream.Collectors.toMap;

public class ArrowCacheProducer extends NoOpFlightProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheProducer.class);

    private static final ActionType DELETE = new ActionType("DELETE", "");

    private static Map<String, DataSchema> createDataSchemaMap(
            BufferAllocator allocator,
            Map<String, RootSchemaConfig.ChildSchemaConfig> schemaConfigMap
    ) {
        return schemaConfigMap.entrySet().stream()
                .collect(toMap(
                        en -> en.getKey(),
                        en -> new DataSchema(allocator, en.getKey(), en.getValue())
                ));
    }

    private final BufferAllocator allocator;

    private final RootSchemaConfig schemaConfig;

    private final Location location;

    private final Duration requestLifetime;

    private final DataSchema rootSchema;

    private final ConcurrentMap<UUID, RequestExecutor> pendingRequests;

    private final Timer requestCleanerTimer = new Timer();

    private volatile boolean closing = false;


    public ArrowCacheProducer(
            BufferAllocator allocator,
            RootSchemaConfig schemaConfig,
            DataSchema rootSchema,
            Location location,
            Duration requestLifetime
    ) {
        this.allocator = allocator.newChildAllocator("ArrowCacheProducer", 0, Integer.MAX_VALUE);
        this.schemaConfig = schemaConfig;
        this.location = location;
        this.requestLifetime = requestLifetime;
        this.rootSchema = rootSchema;
        this.pendingRequests = new ConcurrentHashMap<>();

        ConcurrencyUtils.scheduleAtFixedRate(
                this.requestCleanerTimer,
                this::closeStaleRequests,
                requestLifetime.dividedBy(10)
        );
    }

    @Override
    public void close() {
        logger.info("Closing...");
        closing = true;
        allocator.close();
    }

    private void closeStaleRequests() {
        if (!closing && !pendingRequests.isEmpty()) {
            try {
                logger.info("Checking for timed out requests. {} request(s) pending", pendingRequests.size());

                final Instant cutoff = Instant.now().minus(requestLifetime);
                final Map<UUID, RequestExecutor> timeoutMap =
                        pendingRequests.values().stream()
                                .filter(req -> req.inception().isBefore(cutoff))
                                .collect(toMap(
                                        req -> req.uuid(),
                                        req -> req
                                ));
                timeoutMap.keySet().forEach(pendingRequests::remove);
                timeoutMap.values().stream()
                        .peek(req -> logger.info("Timing out request: {}", req.uuid()))
                        .forEach(RequestExecutor::close);
            } catch (Exception ex) {
                logger.warn("Ignoring exception", ex);
            }
        }
    }

    public DataSchema getDataSchema(List<String> path) {
        return rootSchema.getDataSchema(path)
                .orElseThrow(() ->
                        ArrowServerUtils.exception(
                                CallStatus.NOT_FOUND,
                                logger,
                                "No schema for path : " + path
                        ).toRuntimeException()
                );
    }

    public DataTable getDataTable(List<String> path) {
        return rootSchema.getDataTable(path)
                .orElseThrow(() ->
                        ArrowServerUtils.exception(
                                CallStatus.NOT_FOUND,
                                logger,
                                "No schema for path : " + path
                        ).toRuntimeException()
                );
    }

//
//    private DataTable getDataTable(DataSchema dataSchema, String tableName) {
//        return dataSchema.getTableOpt(tableName)
//                .orElseThrow(() ->
//                        ArrowServerUtils.exception(
//                                CallStatus.NOT_FOUND,
//                                logger,
//                                "No table in Schema '" + dataSchema.name() + "' with name: " + tableName
//                        ).toRuntimeException()
//                );
//    }

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

            for (List<String> schemaPath : criteriaReq.getSchemaPaths()) {
                final DataSchema schema = getDataSchema(schemaPath);

                final Set<String> tableNames = schema.existingTables();

                final FlightEndpoint flightEndpoint = new FlightEndpoint(
                        new Ticket(new byte[]{}),
                        location
                );

                tableNames.forEach(tableName -> {
                    final FlightDescriptor descriptor = FlightDescriptor.path(schema.name(), tableName);
                    final FlightInfo flightInfo = new FlightInfo(
                            schema.getTableOpt(tableName).get().arrowSchema(),
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
                    final DataTable dataTable = getDataTable(getRequest.getPath());
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
                throw ArrowServerUtils.exception(
                        CallStatus.INVALID_ARGUMENT,
                        logger,
                        "Path-based FlightDescriptors  are not supported"
                ).toRuntimeException();
            }
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.exception(CallStatus.INTERNAL, logger, "Unexpected exception")
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
                        ArrowServerUtils.exception(
                                CallStatus.NOT_FOUND,
                                logger,
                                "No pending query found for ticket UUID " + uuid
                        ).toRuntimeException()
                );
            } else {
                requestExecutor.execute(listener);

                pendingRequests.remove(uuid);

                listener.completed();
            }
        } catch (FlightRuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ArrowServerUtils.exception(CallStatus.INTERNAL, logger, "Unexpected exception")
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
                    throw ArrowServerUtils.exception(
                            CallStatus.INVALID_ARGUMENT,
                            logger,
                            "Cannot accept a put operation where the FlightDescriptor is a command - must be a path"
                    ).toRuntimeException();
                } else {
                    final List<String> flightPath = flightDesc.getPath();

                    final List<String> path;
                    if (flightPath.isEmpty()) {
                        throw ExceptionUtils.exception(
                                logger,
                                "Invalid flight path: " + flightPath + ". Must be [<schema-path>, table]"
                        ).create(Exception::new);
                    }

                    final DataTable table = getDataTable(flightPath);

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
                final DataTable table = getDataTable(deleteRequest.getPath());

                table.deleteEntries(deleteRequest.getKeys());

                listener.onCompleted();
            } else {
                listener.onError(
                        ArrowServerUtils.exception(
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
                    ArrowServerUtils.exception(CallStatus.INTERNAL, logger, "Unexpected exception")
                            .withCause(ex)
                            .toRuntimeException()
            );
        }
    }
}
