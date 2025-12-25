package io.nson.arrowcache.server2;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class AllocatorManager implements AutoCloseable {
    private final SchemaConfig.AllocatorMaxSizeConfig allocatorMaxSize;
    private final RootAllocator rootAllocator;

    public AllocatorManager(SchemaConfig.AllocatorMaxSizeConfig allocatorMaxSize, RootAllocator rootAllocator) {
        this.allocatorMaxSize = allocatorMaxSize;
        this.rootAllocator = rootAllocator;
    }

    public AllocatorManager(SchemaConfig.AllocatorMaxSizeConfig allocatorMaxSize) {
        this(allocatorMaxSize, new RootAllocator());
    }

    public BufferAllocator newChildAllocator(String allocatorName) {
        return newChildAllocator(this.rootAllocator, allocatorName);
    }

    public BufferAllocator newChildAllocator(BufferAllocator parent, String allocatorName) {
        final long maxSize = allocatorMaxSize.getAllocatorMaxSize(allocatorName);
        return parent.newChildAllocator(allocatorName, 0, maxSize);
    }

    @Override
    public void close() {
        rootAllocator.close();
    }
}
