package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.server.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DataSchema implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DataSchema.class);

    private final BufferAllocator allocator;
    private final String name;
    private final SchemaConfig schemaConfig;
    private final Map<String, DataTable> tableMap;

    public DataSchema(
            BufferAllocator allocator,
            String name,
            SchemaConfig schemaConfig
    ) {
        this.allocator = allocator.newChildAllocator("ArrowSchema:" + name, 0, Long.MAX_VALUE);
        this.name = name;
        this.schemaConfig = schemaConfig;
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

    public SchemaConfig schemaConfig() {
        return schemaConfig;
    }

    public Map<String, DataTable> getTableMap() {
        return tableMap;
    }
//    public DataTable getTable(String name) {
//        return getTableOpt(name)
//                .orElseThrow(() ->
//                        ArrowServerUtils.notFound(logger, "No table with name: " + name)
//                );
//    }

    public Optional<DataTable> getTableOpt(String name) {
        return Optional.ofNullable(tableMap.get(name))
                .or(() -> Optional.ofNullable(schemaConfig.tables().get(name))
                        .map(tc -> createDataTable(name, tc)));
    }

    private DataTable createDataTable(String name, SchemaConfig.TableConfig tableConfig) {
        final DataTable datatable = new DataTable(allocator, name, tableConfig);
        tableMap.put(name, datatable);
        return datatable;
    }
}
