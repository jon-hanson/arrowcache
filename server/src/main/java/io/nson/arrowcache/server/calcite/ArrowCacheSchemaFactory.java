package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.RootSchemaConfig;
import io.nson.arrowcache.server.cache.DataSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public final class ArrowCacheSchemaFactory implements SchemaFactory, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheSchemaFactory.class);

    public static @Nullable ArrowCacheSchemaFactory INSTANCE;

//    public static void initialise(BufferAllocator allocator, String configName) throws IOException {
//        final RootSchemaConfig schemaConfig = FileUtils.loadFromResource(configName, RootSchemaConfig.CODEC);
//        initialise(allocator, schemaConfig);
//    }

    public static void initialise(BufferAllocator allocator, DataSchema rootSchema) throws IOException {
        if (INSTANCE == null) {
            INSTANCE = new ArrowCacheSchemaFactory(
                    allocator,
                    rootSchema
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
    private final DataSchema rootDataSchema;
    private final ArrowCacheSchema rootArrowCacheSchema;

    public ArrowCacheSchemaFactory(
            BufferAllocator allocator,
            DataSchema rootDataSchema
    ) {
        logger.info("Creating instance");
        this.allocator = allocator.newChildAllocator("ArrowSchemaFactory", 0, Long.MAX_VALUE);
        this.rootDataSchema = rootDataSchema;
        this.rootArrowCacheSchema = new ArrowCacheSchema(
                this.allocator,
                rootDataSchema
        );
    }

    @Override
    public void close() {
        logger.info("Closing...");
        rootArrowCacheSchema.close();
        this.allocator.close();
    }

    @Override
    public ArrowCacheSchema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        if (parentSchema.getParentSchema() == null) {
            return rootArrowCacheSchema;
        } else {
            final DataSchema parentDataSchema = getSchema(parentSchema);
            return new ArrowCacheSchema(allocator, parentDataSchema.getSchema(name));
        }
    }

    public ArrowCacheSchema rootSchema() {
        return rootArrowCacheSchema;
    }

    public ArrowCacheSchema create(String name) {
        return create(null, name, Collections.emptyMap());
    }

    private DataSchema getSchema(SchemaPlus schema) {
        if (schema.getParentSchema() == null) {
            return rootDataSchema;
        } else {
            final DataSchema parent = getSchema(schema.getParentSchema());
            return parent.getSchema(schema.getName());
        }
    }
}
