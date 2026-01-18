package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.server.RootSchemaConfig;
import io.nson.arrowcache.server.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toUnmodifiableMap;

public class DataSchema implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DataSchema.class);

    private static Map<String, DataSchema> createSchemaMap(
            BufferAllocator allocator,
            Map<String, RootSchemaConfig.ChildSchemaConfig> childSchemaConfig
    ) {
        return childSchemaConfig.entrySet().stream()
                .collect(toUnmodifiableMap(
                    en-> en.getKey(),
                    en ->  new DataSchema(allocator, en.getKey(), en.getValue())
                ));
    }

    private final BufferAllocator allocator;
    private final String name;
    private final Map<String, RootSchemaConfig.TableConfig> tableConfigMap;
    private final Map<String, DataSchema> childSchema;
    private final Map<String, DataTable> tableMap;

    public DataSchema(
            BufferAllocator allocator,
            String name,
            SchemaConfig schemaConfig
    ) {
        logger.info("Creating DataSchema {}", name);
        this.allocator = allocator.newChildAllocator("DataSchema:" + name, 0, Long.MAX_VALUE);
        this.name = Objects.requireNonNull(name);
        this.tableConfigMap = schemaConfig.tables();
        this.childSchema = createSchemaMap(allocator, schemaConfig.childSchema());
        this.tableMap = new ConcurrentHashMap<>();
    }

    @Override
    public void close() {
        try {
            logger.info("Closing DataSchema {}...", name);
            childSchema.values().forEach(DataSchema::close);
            tableMap.values().forEach(DataTable::close);
            this.allocator.close();
        } catch (Exception ex) {
            logger.warn("Error while closing", ex);
            throw new RuntimeException("Error while closing", ex);
        }
    }

    public String name() {
        return name;
    }

    public DataSchema getSchema(String name) {
        return Optional.ofNullable(childSchema.get(name))
                .orElseThrow(() -> new IllegalArgumentException("No such schema: " + name));
    }

    public Optional<DataSchema> getDataSchema(List<String> path) {
        return getDataSchema(path, 0);
    }

    public Optional<DataSchema> getDataSchema(List<String> path, int depth) {
        if (depth == path.size()) {
            return Optional.of(this);
        } else {
            return Optional.ofNullable(childSchema.get(path.get(depth)));
        }
    }

    public Optional<DataTable> getDataTable(List<String> schemaPath, String table) {
        return getDataSchema(schemaPath).map(ds -> ds.tableMap.get(table));
    }

    public Set<String> existingTables() {
        return tableMap.keySet();
    }

    public Optional<DataTable> getOrCreateTable(String name) {
        return Optional.ofNullable(tableMap.get(name))
                .or(() -> Optional.ofNullable(tableConfigMap.get(name))
                        .map(tc -> createDataTable(name, tc)));
    }

    private DataTable createDataTable(String name, RootSchemaConfig.TableConfig tableConfig) {
        final DataTable datatable = new DataTable(allocator, name, tableConfig);
        tableMap.put(name, datatable);
        return datatable;
    }
}
