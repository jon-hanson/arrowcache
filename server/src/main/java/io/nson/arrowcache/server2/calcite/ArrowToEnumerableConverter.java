package io.nson.arrowcache.server2.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.primitives.Ints;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of a table in an Arrow data source.
 */
class ArrowToEnumerableConverter
        extends ConverterImpl implements EnumerableRel {

    protected ArrowToEnumerableConverter(RelOptCluster cluster,
                                         RelTraitSet traitSet, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traitSet, input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ArrowToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                                RelMetadataQuery mq) {
        RelOptCost cost = super.computeSelfCost(planner, mq);
        return requireNonNull(cost, "cost").multiplyBy(0.1);
    }

    @Override public Result implement(EnumerableRelImplementor implementor,
                                      Prefer pref) {
        final ArrowRel.Implementor arrowImplementor = new ArrowRel.Implementor();
        arrowImplementor.visitInput(0, getInput());
        PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferArray());

        final RelOptTable table = requireNonNull(arrowImplementor.table, "table");
        final int fieldCount = table.getRowType().getFieldCount();
        return implementor.result(physType,
                Blocks.toBlock(
                        Expressions.call(table.getExpression(ArrowTable.class),
                                ArrowMethod.ARROW_QUERY.method, implementor.getRootExpression(),
                                arrowImplementor.selectFields != null
                                        ? Expressions.call(
                                        BuiltInMethod.IMMUTABLE_INT_LIST_COPY_OF.method,
                                        Expressions.constant(
                                                Ints.toArray(arrowImplementor.selectFields)))
                                        : Expressions.call(
                                        BuiltInMethod.IMMUTABLE_INT_LIST_IDENTITY.method,
                                        Expressions.constant(fieldCount)),
                                Expressions.constant(arrowImplementor.whereClause))));
    }
}
