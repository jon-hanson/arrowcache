package io.nson.arrowcache.server2.calcite;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;
import org.jspecify.annotations.Nullable;

import java.util.*;

public interface ArrowRel extends RelNode {
    Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

    void implement(Implementor var1);

    class Implementor {
        @Nullable List<Integer> selectFields;
        final List<String> whereClause = new ArrayList<>();
        @Nullable RelOptTable table;
        @Nullable ArrowTable arrowTable;

        void addFilters(List<String> predicates) {
            this.whereClause.addAll(predicates);
        }

        void addProjectFields(List<Integer> fields) {
            this.selectFields = ImmutableIntList.copyOf(fields);
        }

        public void visitInput(int ordinal, RelNode input) {
            Preconditions.checkArgument(ordinal == 0);
            ((ArrowRel)input).implement(this);
        }
    }
}
