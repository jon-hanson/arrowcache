package io.nson.arrowcache.server.calcite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

public final class ArrowTableScan extends TableScan implements ArrowRel {
    private final ArrowCacheTable arrowCacheTable;
    private final ImmutableIntList fields;

    ArrowTableScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable relOptTable,
            ArrowCacheTable arrowCacheTable,
            ImmutableIntList fields
    ) {
        super(cluster, traitSet, ImmutableList.of(), relOptTable);
        this.arrowCacheTable = arrowCacheTable;
        this.fields = fields;

        assert this.getConvention() == ArrowRel.CONVENTION;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.isEmpty());
        return this;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", this.fields);
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = this.table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = this.getCluster().getTypeFactory().builder();

        for (int field : this.fields) {
            builder.add(fieldList.get(field));
        }

        return builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(ArrowRules.TO_ENUMERABLE);

        for (RelOptRule rule : ArrowRules.RULES) {
            planner.addRule(rule);
        }
    }

    @Override
    public void implement(ArrowRel.Implementor implementor) {
        implementor.arrowCacheTable = this.arrowCacheTable;
        implementor.table = this.table;
    }
}
