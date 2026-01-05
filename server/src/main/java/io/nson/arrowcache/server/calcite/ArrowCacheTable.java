package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.common.utils.ExceptionUtils;
import io.nson.arrowcache.server.cache.DataTable;
import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ArrowCacheTable extends AbstractTable
        implements TranslatableTable, QueryableTable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowCacheTable.class);

    private final BufferAllocator allocator;
    private final DataTable dataTable;

    public ArrowCacheTable(
            BufferAllocator allocator,
            DataTable dataSchema
    ) {
        this.allocator = allocator;
        this.dataTable = dataSchema;
    }

    @Override
    public void close() {
        logger.info("Closing...");
        this.dataTable.close();
    }

    public String name() {
        return dataTable.name();
    }

    public Schema arrowSchema() {
        return dataTable.arrowSchema();
    }

    public void addBatch(Schema arrowSchema, ArrowRecordBatch arb) {
        this.dataTable.addBatch(arrowSchema, arb);
    }

    public void addBatches(Schema arrowSchema, List<ArrowRecordBatch> arbs) {
        this.dataTable.addBatches(arrowSchema, arbs);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return deduceRowType(this.arrowSchema(), (JavaTypeFactory) typeFactory);
    }

    private static RelDataType deduceRowType(Schema schema, JavaTypeFactory typeFactory) {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Field field : schema.getFields()) {
            builder.add(
                    field.getName(),
                    ArrowFieldTypeFactory.toType(field.getType(), typeFactory)
            );
        }
        return builder.build();
    }

    @SuppressWarnings("unused")
    public Enumerable<Object> query(
            DataContext root,
            ImmutableIntList fields,
            List<String> conditions
    ) {
        Objects.requireNonNull(fields, "fields");

        final Schema arrowSchema = this.arrowSchema();

        final Projector projector;
        final Filter filter;

        if (conditions.isEmpty()) {
            filter = null;

            final List<ExpressionTree> expressionTrees = new ArrayList<>();
            for (int fieldOrdinal : fields) {
                final Field field = arrowSchema.getFields().get(fieldOrdinal);
                final TreeNode node = TreeBuilder.makeField(field);
                expressionTrees.add(TreeBuilder.makeExpression(node, field));
            }

            try {
                projector = Projector.make(arrowSchema, expressionTrees);
            } catch (GandivaException ex) {
                throw Util.toUnchecked(ex);
            }
        } else {
            projector = null;

            final List<TreeNode> conditionNodes = new ArrayList<>(conditions.size());
            for (String condition : conditions) {
                final String[] data = condition.split(" ");
                final List<TreeNode> treeNodes = new ArrayList<>(2);
                treeNodes.add(
                        TreeBuilder.makeField(
                                arrowSchema.getFields()
                                        .get(arrowSchema.getFields()
                                                .indexOf(arrowSchema.findField(data[0]))
                                        )
                        )
                );

                // if the split condition has more than two parts it's a binary operator
                // with an additional literal node
                if (data.length > 2) {
                    treeNodes.add(makeLiteralNode(data[2], data[3]));
                }

                final String operator = data[1];
                conditionNodes.add(TreeBuilder.makeFunction(operator, treeNodes, new ArrowType.Bool()));
            }

            final Condition filterCondition;
            if (conditionNodes.size() == 1) {
                filterCondition = TreeBuilder.makeCondition(conditionNodes.get(0));
            } else {
                final TreeNode treeNode = TreeBuilder.makeAnd(conditionNodes);
                filterCondition = TreeBuilder.makeCondition(treeNode);
            }

            try {
                filter = Filter.make(arrowSchema, filterCondition);
            } catch (GandivaException e) {
                throw Util.toUnchecked(e);
            }
        }

        return new ArrowEnumerable(
                this.allocator,
                arrowSchema,
                this.dataTable.arrowBatches(),
                fields,
                projector,
                filter
        );
    }

    private static TreeNode makeLiteralNode(String literal, String type) {
        if (type.startsWith("decimal")) {
            final String[] typeParts =
                    type.substring(type.indexOf('(') + 1, type.indexOf(')')).split(",");
            final int precision = Integer.parseInt(typeParts[0]);
            final int scale = Integer.parseInt(typeParts[1]);
            return TreeBuilder.makeDecimalLiteral(literal, precision, scale);
        } else {
            switch (type) {
                case "integer":
                    return TreeBuilder.makeLiteral(Integer.parseInt(literal));
                case "long":
                    return TreeBuilder.makeLiteral(Long.parseLong(literal));
                case "float":
                    return TreeBuilder.makeLiteral(Float.parseFloat(literal));
                case "double":
                    return TreeBuilder.makeLiteral(Double.parseDouble(literal));
                case "string":
                    return TreeBuilder.makeStringLiteral(literal.substring(1, literal.length() - 1));
                default:
                    throw ExceptionUtils.exception(
                            logger,
                            "Invalid literal " + literal + ", type " + type
                    ).create(IllegalArgumentException::new);
            }
        }
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final int fieldCount = relOptTable.getRowType().getFieldCount();
        final ImmutableIntList fields = ImmutableIntList.copyOf(Util.range(fieldCount));
        final RelOptCluster cluster = context.getCluster();
        return new ArrowTableScan(
                cluster,
                cluster.traitSetOf(ArrowRel.CONVENTION),
                relOptTable,
                this,
                fields
        );
    }
}
