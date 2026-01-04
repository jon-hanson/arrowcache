package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.server.RootSchemaConfig;
import io.nson.arrowcache.server.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private final Map<String, DataSchema> schemaMap;
    private final Map<String, DataTable> tableMap;

    public DataSchema(
            BufferAllocator allocator,
            String name,
            SchemaConfig schemaConfig
    ) {
        this.allocator = allocator.newChildAllocator("DataSchema:" + name, 0, Long.MAX_VALUE);
        this.name = name;
        this.tableConfigMap = schemaConfig.tables();
        this.schemaMap = createSchemaMap(allocator, schemaConfig.childSchema());
        this.tableMap = new ConcurrentHashMap<>();
    }

    @Override
    public void close() {
        try {
            logger.info("Closing...");
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
        return Optional.ofNullable(schemaMap.get(name))
                .orElseThrow(() -> new IllegalArgumentException("No such schema: " + name));
    }

    public Set<String> existingTables() {
        return tableMap.keySet();
    }

    public Optional<DataTable> getTableOpt(String name) {
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
