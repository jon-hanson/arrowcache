package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.common.utils.ExceptionUtils;
import io.nson.arrowcache.server.cache.DataSchema;
import io.nson.arrowcache.server.cache.DataTable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;

public final class ArrowCacheSchema extends AbstractSchema implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheSchema.class);

    private final BufferAllocator allocator;
    private final DataSchema dataSchema;
    private final Map<String, ArrowCacheTable> tableMap;

    ArrowCacheSchema(
            BufferAllocator allocator,
            DataSchema dataSchema
    ) {
        logger.info("Creating schema for {}",dataSchema.name());

        this.allocator = allocator;
        this.dataSchema = dataSchema;
        this.tableMap = new TreeMap<>();
    }

    @Override
    public void close() {
        logger.info("Closing...");
        this.tableMap.values().forEach(ArrowCacheTable::close);
    }

    public String name() {
        return dataSchema.name();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return (Map<String, Table>)(Map)this.tableMap;
    }

    public Set<String> existingTables() {
        return tableMap.keySet();
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
                    final DataTable dataTable = dataSchema.getTableOpt(tableName)
                            .orElseThrow(() ->
                                    ExceptionUtils.exception(
                                            logger,
                                            "Table '" + tableName + "' not found"
                                    ).create()
                            );
                    return new ArrowCacheTable(this.allocator, dataTable);
                }
        );
        table.addBatches(arrowSchema, arbs);
    }
}
