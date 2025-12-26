package io.nson.arrowcache.server2.calcite;

import io.nson.arrowcache.server2.SchemaConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ArrowSchema extends AbstractSchema implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowSchema.class);

    private final BufferAllocator allocator;
    private final SchemaConfig schemaConfig;
    private final Map<String, ArrowSchema> childSchemaMap;
    private final Map<String, Table> tableMap;

    ArrowSchema(
            BufferAllocator allocator,
            String name,
            SchemaConfig schemaConfig
    ) {
        this.allocator = allocator.newChildAllocator("ArrowSchema:" + name, 0, Long.MAX_VALUE);
        this.schemaConfig = schemaConfig;
        this.childSchemaMap = new TreeMap<>();
        this.tableMap = new TreeMap<>();
    }

    @Override
    public void close() {
        logger.info("Closing...");
        this.tableMap.values().forEach(table -> ((ArrowTable)table).close());
        this.childSchemaMap.values().forEach(ArrowSchema::close);
        this.allocator.close();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return this.tableMap;
    }

    public void addBatches(String tableName, Schema arrowSchema, ArrowRecordBatch arb) {
        this.addBatches(tableName, arrowSchema, List.of(arb));
    }

    public void addBatches(String tableName, Schema arrowSchema, List<ArrowRecordBatch> arbs) {
        final ArrowTable table = (ArrowTable)this.tableMap.computeIfAbsent(
                tableName,
                tn -> {
                    final SchemaConfig.TableConfig tableConfig = this.schemaConfig.tables().get(tableName);
                    return new ArrowTable(tableName, this.allocator, tableConfig, arrowSchema);
                }
        );
        table.addBatches(arrowSchema, arbs);
    }
}
