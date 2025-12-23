package io.nson.arrowcache.server2.calcite;

import io.nson.arrowcache.server2.SchemaConfig;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class ArrowSchema extends AbstractSchema {
    private static final Logger logger = LoggerFactory.getLogger(ArrowSchema.class);

    private final SchemaConfig schemaConfig;
    private final Map<String, ArrowTable> tableMap;

    ArrowSchema(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        this.tableMap = new TreeMap<>();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return super.getTableMap();
    }

    public void addBatches(String tableName, Schema arrowSchema, List<ArrowRecordBatch> arbs) {
        final ArrowTable table = tableMap.computeIfAbsent(
                tableName,
                tn -> {
                    //final SchemaConfig.TableConfig tableConfig = schemaConfig.tables().get(tableName);
                    return new ArrowTable(arrowSchema);
                }
        );
        table.addBatches(arrowSchema, arbs);
    }
}
