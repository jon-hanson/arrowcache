package io.nson.arrowcache.server;

import io.nson.arrowcache.common.JsonCodec;

import java.util.Map;

public final class SchemaConfig {
    public static final String DEFAULT_SCHEMA_NAME = "";

    public static final JsonCodec<SchemaConfig> CODEC = new Codec();

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

    private static class Codec extends JsonCodec<SchemaConfig> {
        public Codec() {
            super(SchemaConfig.class);
        }
    }

    private final Map<String, SchemaConfig> childSchema;
    private final Map<String, TableConfig> tables;

    public SchemaConfig(
            Map<String, SchemaConfig> childSchema,
            Map<String, TableConfig> tables
    ) {
        this.childSchema = childSchema;
        this.tables = tables;
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "childSchema=" + childSchema +
                "tables=" + tables +
                '}';
    }

    public SchemaConfig childSchema(String name) {
        if (childSchema.containsKey(name)) {
            return childSchema.get(name);
        } else if (childSchema.containsKey(DEFAULT_SCHEMA_NAME)) {
            return childSchema.get(DEFAULT_SCHEMA_NAME);
        } else {
            throw new IllegalArgumentException(String.format("Unknown child schema: %s", name));
        }
    }

    public Map<String, TableConfig> tables() {
        return tables;
    }
}
