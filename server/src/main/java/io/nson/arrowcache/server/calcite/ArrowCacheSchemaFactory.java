package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public final class ArrowCacheSchemaFactory implements SchemaFactory, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheSchemaFactory.class);

    public static @Nullable ArrowCacheSchemaFactory INSTANCE;

    public static void initialise(BufferAllocator allocator, String configName) throws IOException {
        final SchemaConfig schemaConfig = FileUtils.loadFromResource(configName, SchemaConfig.CODEC);
        initialise(allocator, schemaConfig);
    }

    public static void initialise(BufferAllocator allocator, SchemaConfig schemaConfig) {
        if (INSTANCE == null) {
            INSTANCE = new ArrowCacheSchemaFactory(
                    allocator,
                    schemaConfig
            );
        } else {
            throw new IllegalStateException("ArrowSchemaFactory is already initialised");
        }
    }

    public static ArrowCacheSchemaFactory instance() {
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
    private final Map<String, ArrowCacheSchema> schemaMap;

    public ArrowCacheSchemaFactory(BufferAllocator allocator, SchemaConfig schemaConfig) {
        logger.info("Creating ArrowSchemaFactory");
        this.allocator = allocator.newChildAllocator("ArrowSchemaFactory", 0, Long.MAX_VALUE);
        this.schemaConfig = schemaConfig;
        this.schemaMap = new TreeMap<>();
    }

    @Override
    public void close() {
        logger.info("Closing...");
        schemaMap.values().forEach(ArrowCacheSchema::close);
        this.allocator.close();
    }

    @Override
    public ArrowCacheSchema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return schemaMap.computeIfAbsent(
                name,
                u -> new ArrowCacheSchema(allocator, parentSchema, name, schemaConfig.childSchema(name))
        );
    }

    public ArrowCacheSchema create(String name) {
        return create(null, name, Collections.emptyMap());
    }

    public ArrowCacheSchema getSchema(String name) {
        return Optional.ofNullable(schemaMap.get(name))
                .orElseThrow(() -> new RuntimeException("Schema " + name + " not found"));
    }
}
