package io.nson.arrowcache.server.cache;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.nson.arrowcache.common.TablePath;
import io.nson.arrowcache.common.utils.JsonCodec;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

public final class SchemaConfig {
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

    private final AllocatorMaxSizeConfig allocatorMaxSizeConfig;
    private final Map<TablePath, TableConfig> tables;

    public SchemaConfig(
            AllocatorMaxSizeConfig allocatorMaxSizeConfig,
            Map<TablePath, TableConfig> tables
    ) {
        this.allocatorMaxSizeConfig = allocatorMaxSizeConfig;
        this.tables = tables;
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "allocatorMaxSizeConfig=" + allocatorMaxSizeConfig +
                "tables=" + tables +
                '}';
    }

    public AllocatorMaxSizeConfig allocatorMaxSizeConfig() {
        return allocatorMaxSizeConfig;
    }

    public Map<TablePath, TableConfig> tables() {
        return tables;
    }

    public Optional<TableConfig> getNode(TablePath path) {
        return tables.keySet().stream()
                .filter(path::match)
                .min(Comparator.comparing(TablePath::wildcardCount))
                .map(tables::get);
    }
}
