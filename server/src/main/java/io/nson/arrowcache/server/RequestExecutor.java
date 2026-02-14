package io.nson.arrowcache.server;

import io.nson.arrowcache.server.cache.DataTable;
import io.nson.arrowcache.server.utils.ByteUtils;
import io.nson.arrowcache.server.utils.CollectionUtils;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

@NullMarked
public abstract class RequestExecutor implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);

    public static GetRequestExecutor getRequestExecutor(
            Location location,
            DataTable dataTable,
            Collection<Object> keys
    ) {
        return new GetRequestExecutor(location, dataTable, new TreeSet<>(keys));
    }

    public static QueryRequestExecutor queryRequestExecutor(String sql) {
        return new QueryRequestExecutor(sql);
    }

    protected final Instant inception;

    protected final UUID uuid;

    protected RequestExecutor(Instant inception, UUID uuid) {
        this.inception = inception;
        this.uuid = uuid;

        logger.info("Creating {}, uuid: {}", getClass().getSimpleName(), uuid);
    }

    protected RequestExecutor() {
        this(Instant.now(), UUID.randomUUID());
    }

    @Override
    public void close() {
        logger.info("Closing {}, uuid: {}", getClass().getSimpleName(), uuid);
    }

    public Instant inception() {
        return inception;
    }

    public UUID uuid() {
        return uuid;
    }

    public abstract FlightInfo getFlightInfo(FlightDescriptor descriptor);

    public abstract void execute(FlightProducer.ServerStreamListener listener);

    public static class GetRequestExecutor extends RequestExecutor {
        private final Location location;
        private final DataTable dataTable;
        private final Set<Object> keys;
        private final int expectedRecordCount;

        public GetRequestExecutor(Location location, DataTable dataTable, Set<Object> keys) {
            this.location = location;
            this.dataTable = dataTable;
            this.keys = keys;
            this.expectedRecordCount = CollectionUtils.intersect(keys, dataTable.keys()).size();
        }

        @Override
        public FlightInfo getFlightInfo(FlightDescriptor descriptor) {
            final Ticket ticket = new Ticket(ByteUtils.asBytes(uuid));
            final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, location);

            return new FlightInfo(
                    dataTable.arrowSchema(),
                    descriptor,
                    Collections.singletonList(flightEndpoint),
                    -1,
                    expectedRecordCount
            );
        }

        @Override
        public void execute(FlightProducer.ServerStreamListener listener) {
            dataTable.get(keys, listener);
        }
    }

    public static class QueryRequestExecutor extends RequestExecutor {
        private final String sql;

        public QueryRequestExecutor(String sql) {
            this.sql = sql;
        }

        @Override
        public FlightInfo getFlightInfo(FlightDescriptor descriptor) {
            return null;
        }

        @Override
        public void execute(FlightProducer.ServerStreamListener listener) {

        }
    }
}
