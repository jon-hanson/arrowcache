package io.nson.arrowcache.server2.calcite;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server2.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.schema.*;
import org.jspecify.annotations.Nullable;
import org.slf4j.*;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public final class ArrowSchemaFactory implements SchemaFactory, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowSchemaFactory.class);

    public static @Nullable ArrowSchemaFactory INSTANCE;

    public static void initialise(BufferAllocator allocator, String configName) throws IOException {
        final SchemaConfig schemaConfig = FileUtils.loadFromResource(configName, SchemaConfig.CODEC);
        initialise(allocator, schemaConfig);
    }

    public static void initialise(BufferAllocator allocator, SchemaConfig schemaConfig) {
        if (INSTANCE == null) {
            INSTANCE = new ArrowSchemaFactory(
                    allocator,
                    schemaConfig
            );
        } else {
            throw new IllegalStateException("ArrowSchemaFactory is already initialised");
        }
    }

    public static ArrowSchemaFactory instance() {
        return INSTANCE;
    }

    public static void shutdown() {
        if (INSTANCE != null) {
            INSTANCE.close();
            INSTANCE = null;
        }
    }

    private final BufferAllocator allocator;
    private final SchemaConfig schemaConfig;
    private final Map<String, ArrowSchema> schemaMap;

    public ArrowSchemaFactory(BufferAllocator allocator, SchemaConfig schemaConfig) {
        logger.info("Creating ArrowSchemaFactory");
        this.allocator = allocator.newChildAllocator("ArrowSchemaFactory", 0, Long.MAX_VALUE);
        this.schemaConfig = schemaConfig;
        this.schemaMap = new TreeMap<>();
    }

    @Override
    public void close() {
        logger.info("Closing...");
        schemaMap.values().forEach(ArrowSchema::close);
        this.allocator.close();
    }

    @Override
    public ArrowSchema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return create(name);
    }

    public ArrowSchema create(String name) {
        return schemaMap.computeIfAbsent(
                name,
                u -> new ArrowSchema(allocator, name, schemaConfig.childSchema(name))
        );
    }
}
