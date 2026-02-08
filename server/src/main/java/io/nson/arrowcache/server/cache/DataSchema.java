package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.server.RootSchemaConfig;
import io.nson.arrowcache.server.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

@NullMarked
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

    private static ConcurrentMap<String, DataTable> createTableMap(
            BufferAllocator allocator,
            Map<String, RootSchemaConfig.TableConfig> tableConfigMap
    ) {
        return tableConfigMap.entrySet().stream()
                .collect(toConcurrentMap(
                        en -> en.getKey(),
                        en -> DataTable.create(allocator, en.getKey(), en.getValue())
                ));
    }

    private final BufferAllocator allocator;
    private final String name;
    private final Map<String, DataSchema> childSchema;
    private final ConcurrentMap<String, DataTable> tableMap;

    public DataSchema(
            BufferAllocator allocator,
            String name,
            SchemaConfig schemaConfig
    ) {
        logger.info("Creating DataSchema {}", name);
        this.allocator = allocator.newChildAllocator("DataSchema:" + name, 0, Long.MAX_VALUE);
        this.name = Objects.requireNonNull(name);
        this.childSchema = createSchemaMap(allocator, schemaConfig.childSchema());
        this.tableMap = createTableMap(allocator, schemaConfig.tables());
    }

    @Override
    public void close() {
        try {
            logger.info("Closing DataSchema {}...", name);
            this.childSchema.values().forEach(DataSchema::close);
            this.tableMap.values().forEach(DataTable::close);
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
        return Optional.ofNullable(this.childSchema.get(name))
                .orElseThrow(() -> new IllegalArgumentException("No such schema: " + name));
    }

    public Optional<DataSchema> getDataSchema(List<String> path) {
        if (path.isEmpty() || !path.get(0).equals(name)) {
            return Optional.empty();
        } else {
            return getDataSchema(path, 1);
        }
    }

    public Optional<DataSchema> getDataSchema(List<String> path, int depth) {
        if (depth == path.size()) {
            return Optional.of(this);
        } else {
            return Optional.ofNullable(this.childSchema.get(path.get(depth)))
                    .flatMap(ds -> ds.getDataSchema(path, depth + 1));
        }
    }

    public Set<String> dataTableNames() {
        return this.tableMap.keySet();
    }

    public Optional<DataTable> getDataTable(String table) {
        return Optional.ofNullable(this.tableMap.get(table));
    }

    public void mergeEachTable() {
        this.tableMap.values().forEach(DataTable::mergeBatches);
    }

    public void mergeEachTable(Collection<String> tables) {
        synchronized(this.tableMap) {
            final Set<String> tables2 = new HashSet<>(tables);
            tables2.removeAll(this.tableMap.keySet());
            if (!tables2.isEmpty()) {
                throw new IllegalArgumentException("The following tables are not found: " + tables2);
            } else {
                tables.forEach(table -> this.tableMap.get(table).mergeBatches());
            }
        }
    }
}
