package io.nson.arrowcache.server2.calcite;

import io.nson.arrowcache.server2.SchemaConfig;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ArrowSchema extends AbstractSchema implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ArrowSchema.class);

    private final SchemaConfig schemaConfig;
    private final Map<String, Table> tableMap;

    ArrowSchema(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        this.tableMap = new TreeMap<>();
    }

    @Override
    public void close() {
        tableMap.values().forEach(table -> ((ArrowTable)table).close());
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    public void addBatches(String tableName, Schema arrowSchema, ArrowRecordBatch arb) {
        this.addBatches(tableName, arrowSchema, List.of(arb));
    }

    public void addBatches(String tableName, Schema arrowSchema, List<ArrowRecordBatch> arbs) {
        final ArrowTable table = (ArrowTable)tableMap.computeIfAbsent(
                tableName,
                tn -> {
                    //final SchemaConfig.TableConfig tableConfig = schemaConfig.tables().get(tableName);
                    return new ArrowTable(arrowSchema);
                }
        );
        table.addBatches(arrowSchema, arbs);
    }
}
