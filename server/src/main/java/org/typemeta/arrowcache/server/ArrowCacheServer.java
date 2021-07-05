package org.typemeta.arrowcache.server;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.example.InMemoryStore;
import org.apache.arrow.memory.*;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.*;

import java.io.IOException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowCacheServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheServer.class);

    private final FlightServer flightServer;
    private final Location location;
    private final BufferAllocator allocator;
    private final InMemoryStore mem;

    public ArrowCacheServer(BufferAllocator allocator, Location location) {
        this.allocator = allocator.newChildAllocator("flight-server", 0L, 9223372036854775807L);
        this.location = location;
        this.mem = new InMemoryStore(this.allocator, location);
        this.flightServer = FlightServer.builder(allocator, location, this.mem).build();
    }

    public Location getLocation() {
        return this.location;
    }

    public int getPort() {
        return this.flightServer.getPort();
    }

    public void start() throws IOException {
        this.flightServer.start();
    }

    public void awaitTermination() throws InterruptedException {
        this.flightServer.awaitTermination();
    }

    public InMemoryStore getStore() {
        return this.mem;
    }

    public void close() throws Exception {
        AutoCloseables.close(this.mem, this.flightServer, this.allocator);
    }

    public static void main(String[] args) throws Exception {
        final BufferAllocator a = new RootAllocator(9223372036854775807L);
        final ArrowCacheServer efs = new ArrowCacheServer(a, Location.forGrpcInsecure("localhost", 12233));
        efs.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nExiting...");
                AutoCloseables.close(efs, a);
            } catch (Exception var3) {
                var3.printStackTrace();
            }

        }));
        efs.awaitTermination();
    }
}
