package io.nson.arrowcache.server;

import io.nson.arrowcache.server.cache.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class AllocatorManager implements AutoCloseable {
    private final SchemaConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig;
    private final RootAllocator rootAllocator;

    public AllocatorManager(SchemaConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig, RootAllocator rootAllocator) {
        this.allocatorMaxSizeConfig = allocatorMaxSizeConfig;
        this.rootAllocator = rootAllocator;
    }

    public AllocatorManager(SchemaConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig) {
        this(allocatorMaxSizeConfig, new RootAllocator());
    }

    public BufferAllocator newChildAllocator(String allocatorName) {
        return newChildAllocator(this.rootAllocator, allocatorName);
    }

    public BufferAllocator newChildAllocator(BufferAllocator parent, String allocatorName) {
        final long maxSize = allocatorMaxSizeConfig.getAllocatorMaxSize(allocatorName);
        return parent.newChildAllocator(allocatorName, 0, maxSize);
    }

    @Override
    public void close() {
        rootAllocator.close();
    }
}
