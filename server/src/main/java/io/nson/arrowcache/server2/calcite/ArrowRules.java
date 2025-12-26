package io.nson.arrowcache.server2.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.google.common.collect.ImmutableList;
import org.jspecify.annotations.Nullable;

//import org.immutables.value.Value;

import java.util.List;

/** Planner rules relating to the Arrow adapter. */
public class ArrowRules {
    private ArrowRules() {}

    /** Rule that matches a {@link org.apache.calcite.rel.core.Project} on
     * an {@link ArrowTableScan} and pushes down projects if possible. */
    public static final ArrowProjectRule PROJECT_SCAN =
            ArrowProjectRule.DEFAULT_CONFIG.toRule(ArrowProjectRule.class);

    public static final ArrowFilterRule FILTER_SCAN =
            ArrowFilterRule.Config.DEFAULT.toRule();

    public static final ConverterRule TO_ENUMERABLE =
            ArrowToEnumerableConverterRule.DEFAULT_CONFIG
                    .toRule(ArrowToEnumerableConverterRule.class);

    public static final List<RelOptRule> RULES = ImmutableList.of(PROJECT_SCAN, FILTER_SCAN);

    /** Base class for planner rules that convert a relational expression to
     * the Arrow calling convention. */
    abstract static class ArrowConverterRule extends ConverterRule {
        ArrowConverterRule(Config config) {
            super(config);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Filter} to an
     * {@link ArrowFilter}.
     */
    public static class ArrowFilterRule extends RelRule<ArrowFilterRule.Config> {

        /** Creates an ArrowFilterRule. */
        protected ArrowFilterRule(Config config) {
            super(config);
        }

        @Override public void onMatch(RelOptRuleCall call) {
            final Filter filter = call.rel(0);

            if (filter.getTraitSet().contains(Convention.NONE)) {
                try {
                    final RelNode converted = convert(filter);
                    call.transformTo(converted);
                } catch (UnsupportedOperationException e) {
                    // skip rule application when hitting an unsupported feature,
                    // allowing a plan in the Enumerable convention to be generated
                }
            }
        }

        RelNode convert(Filter filter) {
            final RelTraitSet traitSet =
                    filter.getTraitSet().replace(ArrowRel.CONVENTION);
            return new ArrowFilter(filter.getCluster(), traitSet,
                    convert(filter.getInput(), ArrowRel.CONVENTION),
                    filter.getCondition());
        }

        /** Rule configuration. */
//        @Value.Immutable
        public interface Config extends RelRule.Config {
            Config DEFAULT = ImmutableConfig.builder()
                    .withOperandSupplier(b0 ->
                            b0.operand(LogicalFilter.class).oneInput(b1 ->
                                    b1.operand(ArrowTableScan.class).noInputs()))
                    .build();

            @Override default ArrowFilterRule toRule() {
                return new ArrowFilterRule(this);
            }
        }
    }

    /**
     * Planner rule that projects from a {@link ArrowTableScan} just the columns
     * needed to satisfy a projection. If the projection's expressions are
     * trivial, the projection is removed.
     *
     * @see ArrowRules#PROJECT_SCAN
     */
    public static class ArrowProjectRule extends ArrowConverterRule {

        /** Default configuration. */
        protected static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalProject.class, Convention.NONE,
                        ArrowRel.CONVENTION, "ArrowProjectRule")
                .withRuleFactory(ArrowProjectRule::new);

        /** Creates an ArrowProjectRule. */
        protected ArrowProjectRule(Config config) {
            super(config);
        }

        @Override public @Nullable RelNode convert(RelNode rel) {
            final Project project = (Project) rel;
            List<Integer> fields = ArrowProject.getProjectFields(project.getProjects());
            if (fields == null) {
                // Project contains expressions more complex than just field references.
                return null;
            }
            final RelTraitSet traitSet = project.getTraitSet().replace(ArrowRel.CONVENTION);
            return new ArrowProject(
                    project.getCluster(),
                    traitSet,
                    convert(project.getInput(), ArrowRel.CONVENTION),
                    project.getProjects(),
                    project.getRowType()
            );
        }
    }

    /**
     * Rule to convert a relational expression from
     * {@link ArrowRel#CONVENTION} to {@link EnumerableConvention}.
     */
    static class ArrowToEnumerableConverterRule extends ConverterRule {

        /** Default configuration. */
        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(
                        RelNode.class,
                        ArrowRel.CONVENTION,
                        EnumerableConvention.INSTANCE,
                        "ArrowToEnumerableConverterRule"
                ).withRuleFactory(ArrowToEnumerableConverterRule::new);

        /** Creates an ArrowToEnumerableConverterRule. */
        protected ArrowToEnumerableConverterRule(Config config) {
            super(config);
        }

        @Override public RelNode convert(RelNode rel) {
            RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
            return new ArrowToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
        }
    }
}
