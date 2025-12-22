package io.nson.arrowcache.server2;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.arrow.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

class ArrowTableScan extends TableScan implements ArrowRel {
    private final ArrowTable arrowTable;
    private final ImmutableIntList fields;

    ArrowTableScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable relOptTable,
            ArrowTable arrowTable,
            ImmutableIntList fields
    ) {
        super(cluster, traitSet, ImmutableList.of(), relOptTable);
        this.arrowTable = arrowTable;
        this.fields = fields;

        assert this.getConvention() == ArrowRel.CONVENTION;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.isEmpty());
        return this;
    }

    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", this.fields);
    }

    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = this.table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = this.getCluster().getTypeFactory().builder();

        for (int field : this.fields) {
            builder.add(fieldList.get(field));
        }

        return builder.build();
    }

    public void register(RelOptPlanner planner) {
        planner.addRule(ArrowRules.TO_ENUMERABLE);

        for (RelOptRule rule : ArrowRules.RULES) {
            planner.addRule(rule);
        }
    }

    public void implement(ArrowRel.Implementor implementor) {
        implementor.arrowTable = this.arrowTable;
        implementor.table = this.table;
    }
}
