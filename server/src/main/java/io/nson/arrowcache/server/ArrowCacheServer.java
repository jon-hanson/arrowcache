package io.nson.arrowcache.server;

import io.nson.arrowcache.server.cache.DataStore;
import org.apache.arrow.flight.*;
import org.apache.arrow.util.AutoCloseables;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowCacheServer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheServer.class);

    private final FlightServer flightServer;
    private final ArrowCacheProducer flightProducer;
    private final Location location;
    private final BufferAllocator allocator;

    public ArrowCacheServer(DataStore dataStore, BufferAllocator allocator, Location location) {
        this.allocator = allocator.newChildAllocator("flight-server", 0L, 9223372036854775807L);
        this.location = location;
        this.flightProducer = new ArrowCacheProducer(dataStore, allocator, location);
        this.flightServer = FlightServer.builder(allocator, location, flightProducer).build();

        logger.info("New instance for location {}", location);
    }

    public void close() throws Exception {
        logger.info("Instance closing for location {}", location);
        AutoCloseables.close(this.flightProducer);
        AutoCloseables.close(this.flightServer);
        AutoCloseables.close(this.allocator);
    }

    public Location location() {
        return this.location;
    }

    public int port() {
        return this.flightServer.getPort();
    }

    public void start() throws IOException {
        this.flightServer.start();
    }

    public void awaitTermination() throws InterruptedException {
        this.flightServer.awaitTermination();
    }

    /*
    Run with
        --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
        --add-opens=java.base/java.nio=org.apache.arrow.flight.core,ALL-UNNAMED
    and for memory debugging:
        -Darrow.memory.debug.allocator=true
     */
    public static void main(String[] args) throws Exception {
        logger.info("Starting");

        final BufferAllocator buffAlloc = new RootAllocator();

        final DataStore dataStore = new DataStore(null, buffAlloc);
        final ArrowCacheServer server = new ArrowCacheServer(
                dataStore,
                buffAlloc,
                Location.forGrpcInsecure("localhost", 12233)
        );

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Exiting...");
                AutoCloseables.close(server);
                AutoCloseables.close(buffAlloc);
            } catch (Exception ex) {
                logger.error("Ignoring exception", ex);
            }
        }));

        logger.info("Awaiting termination");
        server.awaitTermination();

        logger.info("Stopping");
    }
}
