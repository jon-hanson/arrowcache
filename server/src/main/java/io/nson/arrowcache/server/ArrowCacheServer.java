package io.nson.arrowcache.server;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.cache.SchemaConfig;
import io.nson.arrowcache.server.cache.DataStore;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ArrowCacheServer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheServer.class);

    private final BufferAllocator allocator;
    private final Location location;
    private final ArrowCacheProducer flightProducer;
    private final FlightServer flightServer;

    public ArrowCacheServer(
            AllocatorManager allocatorManager,
            Location location,
            DataStore dataStore
    ) {
        this.allocator = allocatorManager.newChildAllocator("ArrowCacheServer");
        this.location = location;
        this.flightProducer = new ArrowCacheProducer(dataStore, location);
        this.flightServer = FlightServer.builder(allocator, location, flightProducer).build();

        logger.info("New instance for location {}", location);
    }

    public void close() throws Exception {
        logger.info("Closing for location {}...", location);
        AutoCloseables.close(
                this.flightServer,
                this.flightProducer,
                this.allocator
        );
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

        final SchemaConfig schemaConfig = FileUtils.loadFromResource("cacheconfig.json", SchemaConfig.CODEC);
        final ServerConfig serverConfig = FileUtils.loadFromResource("serverconfig.json", ServerConfig.CODEC);
        final AllocatorManager allocatorManager = new AllocatorManager(schemaConfig.allocatorMaxSizeConfig());

        final DataStore dataStore = new DataStore(schemaConfig, allocatorManager);
        final ArrowCacheServer server = new ArrowCacheServer(
                allocatorManager,
                Location.forGrpcInsecure("localhost", serverConfig.port()),
                dataStore
        );

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Shutting down...");
                AutoCloseables.close(dataStore, server, allocatorManager);
            } catch (Exception ex) {
                logger.error("Ignoring exception", ex);
            }
        }));

        logger.info("Awaiting termination");
        server.awaitTermination();

        logger.info("Exiting");
    }
}
