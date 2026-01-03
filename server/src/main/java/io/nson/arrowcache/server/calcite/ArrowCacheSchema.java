package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.server.SchemaConfig;
import io.nson.arrowcache.server.cache.DataSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class ArrowCacheSchema extends AbstractSchema implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheSchema.class);

    private final BufferAllocator allocator;
    private final DataSchema dataSchema;
    private final SchemaPlus parent;
    private final Map<String, ArrowCacheSchema> childSchemaMap;
    private final Map<String, ArrowCacheTable> tableMap;

    ArrowCacheSchema(
            BufferAllocator allocator,
            SchemaPlus parent,
            String name,
            SchemaConfig schemaConfig
    ) {
        logger.info(
                "Creating schema for {} with parent {}",
                name,
                Optional.ofNullable(parent).map(SchemaPlus::getName).orElse("<null>")
        );

        this.allocator = allocator;
        this.dataSchema = new DataSchema(allocator, name, schemaConfig);
        this.parent = parent;
        this.childSchemaMap = new TreeMap<>();
        this.tableMap = new TreeMap<>();
    }

    @Override
    public void close() {
        logger.info("Closing...");
        this.tableMap.values().forEach(table -> ((ArrowCacheTable) table).close());
        this.dataSchema.close();
        this.childSchemaMap.values().forEach(ArrowCacheSchema::close);
    }

    public String name() {
        return dataSchema.name();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return (Map<String, Table>)(Map)this.tableMap;
    }

    public ArrowCacheTable table(String name) {
        return this.tableMap.get(name);
    }

    public Set<ArrowCacheTable> getTables() {
        return new HashSet<>(tableMap.values());
    }

    public void addBatches(String tableName, Schema arrowSchema, ArrowRecordBatch arb) {
        this.addBatches(tableName, arrowSchema, List.of(arb));
    }

    public void addBatches(String tableName, Schema arrowSchema, List<ArrowRecordBatch> arbs) {
        final ArrowCacheTable table = this.tableMap.computeIfAbsent(
                tableName,
                tn -> {
                    final SchemaConfig.TableConfig tableConfig = this.dataSchema.schemaConfig().tables().get(tableName);
                    return new ArrowCacheTable(this.allocator, tableName, tableConfig, arrowSchema);
                }
        );
        table.addBatches(arrowSchema, arbs);
    }
}
