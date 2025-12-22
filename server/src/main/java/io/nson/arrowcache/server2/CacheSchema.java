package io.nson.arrowcache.server2;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.TreeMap;

public class CacheSchema extends AbstractSchema {

    private final Map<String, Table> tables = new TreeMap<>();

    CacheSchema() {

    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }
}
