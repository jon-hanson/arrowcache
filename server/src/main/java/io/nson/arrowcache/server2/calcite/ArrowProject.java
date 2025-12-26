package io.nson.arrowcache.server2.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in Arrow.
 */
final class ArrowProject extends Project implements ArrowRel {

    ArrowProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType
    ) {
        super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
        assert getConvention() == ArrowRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public Project copy(
            RelTraitSet traitSet,
            RelNode input,
            List<RexNode> projects,
            RelDataType rowType
    ) {
        return new ArrowProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(
            RelOptPlanner planner,
            RelMetadataQuery mq
    ) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return requireNonNull(cost, "cost").multiplyBy(0.1);
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitInput(0, getInput());
        final List<Integer> projectedFields = getProjectFields(getProjects());
        if (projectedFields != null) {
            implementor.addProjectFields(projectedFields);
        }
    }

    static @Nullable List<Integer> getProjectFields(List<RexNode> exps) {
        final List<Integer> fields = new ArrayList<>();
        for (final RexNode exp : exps) {
            if (exp instanceof RexInputRef) {
                fields.add(((RexInputRef) exp).getIndex());
            } else {
                // not a simple projection
                return null;
            }
        }
        return fields;
    }
}
