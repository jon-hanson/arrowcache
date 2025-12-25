package io.nson.arrowcache.server2.calcite;

import io.nson.arrowcache.server.cache.*;
import io.nson.arrowcache.server2.SchemaConfig;
import org.apache.arrow.gandiva.evaluator.*;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.*;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.locks.*;

public class ArrowTable extends AbstractTable
        implements TranslatableTable, QueryableTable, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowTable.class);

    private static int findKeyColumn(Schema schema, String keyName) {
        final List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i).getName().equals(keyName)) {
                return i;
            }
        }

        logger.error("Key column name '{}' not found in schema", keyName);
        throw new RuntimeException("Key column name '" + keyName + "' not found in schema");
    }

    private final BufferAllocator allocator;
    private final Schema arrowSchema;
    private String keyColumnName;
    private int keyColumnIndex;
    private final List<ArrowRecordBatch> arrowBatches;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public ArrowTable(
            BufferAllocator allocator,
            SchemaConfig.TableConfig tableConfig,
            Schema arrowSchema
    ) {
        this.allocator = allocator;
        this.arrowSchema = arrowSchema;
        this.keyColumnName = tableConfig.keyColumn();
        this.keyColumnIndex = findKeyColumn(arrowSchema, keyColumnName);
        this.arrowBatches = new ArrayList<>();
    }

    @Override
    public void close() {
        arrowBatches.forEach(ArrowRecordBatch::close);
    }

    public void addBatches(Schema arrowSchema, Collection<ArrowRecordBatch> arbs) {
        if (!this.arrowSchema.equals(arrowSchema)) {
            throw new IllegalArgumentException("Cannot add batches with a schema different to the schema for this table");
        } else {
            synchronized (this.rwLock.writeLock()) {
                this.arrowBatches.addAll(arbs);
            }
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return deduceRowType(this.arrowSchema, (JavaTypeFactory) typeFactory);
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

        final Projector projector;
        final Filter filter;

        if (conditions.isEmpty()) {
            filter = null;

            final List<ExpressionTree> expressionTrees = new ArrayList<>();
            for (int fieldOrdinal : fields) {
                final Field field = this.arrowSchema.getFields().get(fieldOrdinal);
                final TreeNode node = TreeBuilder.makeField(field);
                expressionTrees.add(TreeBuilder.makeExpression(node, field));
            }
            try {
                projector = Projector.make(this.arrowSchema, expressionTrees);
            } catch (GandivaException ex) {
                throw Util.toUnchecked(ex);
            }
        } else {
            projector = null;

            final List<TreeNode> conditionNodes = new ArrayList<>(conditions.size());
            for (String condition : conditions) {
                String[] data = condition.split(" ");
                List<TreeNode> treeNodes = new ArrayList<>(2);
                treeNodes.add(
                        TreeBuilder.makeField(this.arrowSchema.getFields()
                                .get(this.arrowSchema.getFields().indexOf(this.arrowSchema.findField(data[0])))));

                // if the split condition has more than two parts it's a binary operator
                // with an additional literal node
                if (data.length > 2) {
                    treeNodes.add(makeLiteralNode(data[2], data[3]));
                }

                final String operator = data[1];
                conditionNodes.add(
                        TreeBuilder.makeFunction(operator, treeNodes, new ArrowType.Bool()));
            }

            final Condition filterCondition;
            if (conditionNodes.size() == 1) {
                filterCondition = TreeBuilder.makeCondition(conditionNodes.get(0));
            } else {
                TreeNode treeNode = TreeBuilder.makeAnd(conditionNodes);
                filterCondition = TreeBuilder.makeCondition(treeNode);
            }

            try {
                filter = Filter.make(this.arrowSchema, filterCondition);
            } catch (GandivaException e) {
                throw Util.toUnchecked(e);
            }
        }

        return new ArrowEnumerable(
                this.allocator,
                this.arrowSchema,
                this.arrowBatches,
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
        } else if (type.equals("integer")) {
            return TreeBuilder.makeLiteral(Integer.parseInt(literal));
        } else if (type.equals("long")) {
            return TreeBuilder.makeLiteral(Long.parseLong(literal));
        } else if (type.equals("float")) {
            return TreeBuilder.makeLiteral(Float.parseFloat(literal));
        } else if (type.equals("double")) {
            return TreeBuilder.makeLiteral(Double.parseDouble(literal));
        } else if (type.equals("string")) {
            return TreeBuilder.makeStringLiteral(literal.substring(1, literal.length() - 1));
        } else {
            throw new IllegalArgumentException(
                    "Invalid literal " + literal + ", type " + type
            );
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
        final ImmutableIntList fields =
                ImmutableIntList.copyOf(Util.range(fieldCount));
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
