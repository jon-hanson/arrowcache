package io.nson.arrowcache.server;

import io.nson.arrowcache.common.JsonCodec;
import org.jspecify.annotations.NullMarked;

import java.util.Map;

@NullMarked
public final class RootSchemaConfig extends SchemaConfig {

    public static final JsonCodec<RootSchemaConfig> CODEC = new JsonCodec<>(RootSchemaConfig.class) {};

    public static final class TableConfig {
        private final String keyColumn;

        public TableConfig(String keyColumn) {
            this.keyColumn = keyColumn;
        }

        @Override
        public String toString() {
            return "SchemaConfig{" +
                    "keyColumn='" + keyColumn + '\'' +
                    '}';
        }

        public String keyColumn() {
            return keyColumn;
        }
    }

    public static final class ChildSchemaConfig extends SchemaConfig {
        public ChildSchemaConfig(Map<String, ChildSchemaConfig> childSchema, Map<String, TableConfig> tables) {
            super(childSchema, tables);
        }
    }

    public RootSchemaConfig(
            String name,
            Map<String, ChildSchemaConfig> childSchema,
            Map<String, TableConfig> tables
    ) {
        super(childSchema, tables);
        this.name = name;
    }

    private final String name;

    public String name() {
        return name;
    }
}
