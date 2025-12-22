package io.nson.arrowcache.server2;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

public class CacheTable extends AbstractTable
        implements TranslatableTable, QueryableTable {
    private static final Logger logger = LoggerFactory.getLogger(CacheTable.class);

    private final @Nullable RelProtoDataType protoRowType;
    private final Schema arrowSchema;

    public CacheTable(@Nullable RelProtoDataType protoRowType, Schema arrowSchema) {
        this.protoRowType = protoRowType;
        this.arrowSchema = arrowSchema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return null;
    }

    @Override
    public Type getElementType() {
        return null;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return null;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return null;
    }

    @Override
    public <C> C unwrapOrThrow(Class<C> aClass) {
        return super.unwrapOrThrow(aClass);
    }
}
