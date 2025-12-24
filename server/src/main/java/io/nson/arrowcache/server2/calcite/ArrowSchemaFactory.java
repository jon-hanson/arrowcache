package io.nson.arrowcache.server2.calcite;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server2.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.schema.*;
import org.jspecify.annotations.Nullable;
import org.slf4j.*;

import java.io.IOException;
import java.util.Map;

public class ArrowSchemaFactory implements SchemaFactory, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowSchemaFactory.class);

    private static BufferAllocator allocator;
    private static @Nullable ArrowSchema rootSchema;

    public static void initialise(BufferAllocator allocator) {
        ArrowSchemaFactory.allocator = allocator;
    }

    public ArrowSchemaFactory() {
        logger.info("Creating ArrowSchemaFactory");
    }

    @Override
    public void close() {
        if (rootSchema != null) {
            rootSchema.close();
            rootSchema = null;
        }
    }

    public static ArrowSchema rootSchema() {
        if (rootSchema == null) {
            try {
                final SchemaConfig schemaConfig = FileUtils.loadFromResource("schemaconfig.json", SchemaConfig.CODEC);
                rootSchema = new ArrowSchema(allocator, schemaConfig);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        return rootSchema;
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        logger.info("create called for name '{}'", name);

        if (rootSchema == null) {
            try {
                final String schemaConfigName = (String) operand.get("schemaConfig");
                final SchemaConfig schemaConfig = FileUtils.loadFromResource(schemaConfigName, SchemaConfig.CODEC);
                rootSchema = new ArrowSchema(allocator, schemaConfig);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        return rootSchema;
    }
}
