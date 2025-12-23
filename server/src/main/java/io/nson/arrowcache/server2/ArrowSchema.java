package io.nson.arrowcache.server2;

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.stream.Collectors.toMap;

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

    public void add(String tableName, Schema arrowSchema, List<ArrowRecordBatch> arbs) {
        ArrowTable table = tableMap.get(tableName);
        if (table == null) {
            final SchemaConfig.TableConfig tableConfig = schemaConfig.tables().get(tableName);

        }
    }
}
