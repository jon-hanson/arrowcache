package io.nson.arrowcache.server.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;

public class CalciteDataContext implements DataContext {
    private final SchemaPlus rootSchema;
    private final JavaTypeFactory typeFactory;

    public CalciteDataContext(SchemaPlus rootSchema, RelNode rel) {
        this.rootSchema = rootSchema;
        this.typeFactory = (JavaTypeFactory) rel.getCluster().getTypeFactory();
    }

    @Override
    public SchemaPlus getRootSchema() {
        return rootSchema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(String name) {
        return null;
    }
}
