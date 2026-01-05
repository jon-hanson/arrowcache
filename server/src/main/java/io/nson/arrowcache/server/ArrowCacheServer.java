package io.nson.arrowcache.server;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.cache.DataSchema;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ArrowCacheServer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheServer.class);

    private final BufferAllocator allocator;
    private final Location location;
    private final ArrowCacheProducer flightProducer;
    private final FlightServer flightServer;

    public ArrowCacheServer(
            BufferAllocator allocator,
            Location location,
            Duration requestLifetime,
            RootSchemaConfig schemaConfig,
            DataSchema rootSchema
    ) {
        this.allocator = allocator.newChildAllocator("ArrowCacheServer", 0, Integer.MAX_VALUE);
        this.location = location;
        this.flightProducer = new ArrowCacheProducer(allocator, schemaConfig, rootSchema, location, requestLifetime);
        this.flightServer = FlightServer.builder(allocator, location, flightProducer).build();

        logger.info("New server for location {}", location);
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

        final RootSchemaConfig schemaConfig =
                FileUtils.loadFromResource(
                        "schemaconfig.json",
                        RootSchemaConfig.CODEC
                );
        final ServerConfig serverConfig =
                FileUtils.loadFromResource(
                        "serverconfig.json",
                        ServerConfig.CODEC
                );

        final Location location = Location.forGrpcInsecure("localhost", serverConfig.serverPort());
        final RootAllocator allocator = new RootAllocator();
        final DataSchema rootSchema = new DataSchema(allocator, schemaConfig.name(), schemaConfig);

        final ArrowCacheServer server = new ArrowCacheServer(
                allocator,
                location,
                serverConfig.requestLifetime(),
                schemaConfig,
                rootSchema
        );

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Shutting down...");
                AutoCloseables.close(server, rootSchema, allocator);
            } catch (Exception ex) {
                logger.error("Ignoring exception", ex);
            }
        }));

        logger.info("Awaiting termination");
        server.awaitTermination();

        logger.info("Exiting");
    }
}
