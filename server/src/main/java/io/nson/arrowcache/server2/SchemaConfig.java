package io.nson.arrowcache.server2;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.nson.arrowcache.common.TablePath;
import io.nson.arrowcache.common.utils.JsonCodec;

import java.io.IOException;
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

    public static final class AllocatorMaxSizeConfig {
        private final long defaultSize;
        private final Map<String, Long> allocatorSizeMap;

        public AllocatorMaxSizeConfig(long defaultSize, Map<String, Long> allocatorSizeMap) {
            this.defaultSize = defaultSize;
            this.allocatorSizeMap = allocatorSizeMap;
        }

        public long defaultSize() {
            return defaultSize;
        }

        public Map<String, Long> allocatorSizeMap() {
            return allocatorSizeMap;
        }

        public long getAllocatorMaxSize(String allocatorName) {
            final Long value = allocatorSizeMap.get(allocatorName);
            return value == null ? defaultSize : value;
        }
    }

    private static class Codec extends JsonCodec<SchemaConfig> {
        private static class TablePathTypeAdaptor extends TypeAdapter<TablePath> {

            @Override
            public void write(JsonWriter jsonWriter, TablePath tablePath) throws IOException {
                jsonWriter.value(tablePath.toString());
            }

            @Override
            public TablePath read(JsonReader jsonReader) throws IOException {
                return TablePath.valueOfConcat(jsonReader.nextString());
            }
        }

        public Codec() {
            super(SchemaConfig.class);
        }

        @Override
        protected GsonBuilder prepare(GsonBuilder gsonBuilder) {
            return gsonBuilder.registerTypeAdapter(TablePath.class, new TablePathTypeAdaptor());
        }
    }

    private final AllocatorMaxSizeConfig allocatorMaxSize;
    private final Map<String, SchemaConfig> childSchema;
    private final Map<String, TableConfig> tables;

    public SchemaConfig(
            AllocatorMaxSizeConfig allocatorMaxSize,
            Map<String, SchemaConfig> childSchema,
            Map<String, TableConfig> tables
    ) {
        this.allocatorMaxSize = allocatorMaxSize;
        this.childSchema = childSchema;
        this.tables = tables;
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "allocatorMaxSize=" + allocatorMaxSize +
                "childSchema=" + childSchema +
                "tables=" + tables +
                '}';
    }

    public AllocatorMaxSizeConfig allocatorMaxSize() {
        return allocatorMaxSize;
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
