package io.nson.arrowcache.server2.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Arrow.
 */
class ArrowFilter extends Filter implements ArrowRel {
    private final List<String> match;

    ArrowFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) {
        super(cluster, traitSet, input, condition);
        final ArrowTranslator translator =
                ArrowTranslator.create(cluster.getRexBuilder(), input.getRowType());
        this.match = translator.translateMatch(condition);

        assert getConvention() == ArrowRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return Objects.requireNonNull(cost, "cost").multiplyBy(0.1);
    }

    @Override public ArrowFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new ArrowFilter(getCluster(), traitSet, input, condition);
    }

    @Override public void implement(Implementor implementor) {
        implementor.visitInput(0, getInput());
        implementor.addFilters(match);
    }
}
