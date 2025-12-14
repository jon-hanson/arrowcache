package io.nson.arrowcache.server;

import io.nson.arrowcache.server.cache.CacheConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class AllocatorManager implements AutoCloseable {
    private final CacheConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig;
    private final RootAllocator rootAllocator;

    public AllocatorManager(CacheConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig, RootAllocator rootAllocator) {
        this.allocatorMaxSizeConfig = allocatorMaxSizeConfig;
        this.rootAllocator = rootAllocator;
    }

    public AllocatorManager(CacheConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig) {
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
    public void close() throws Exception {
        rootAllocator.close();
    }
}
