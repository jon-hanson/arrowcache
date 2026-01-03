package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.common.utils.ExceptionUtils;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.DateString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.DateTimeStringUtils.ISO_DATETIME_FRACTIONAL_SECOND_FORMAT;
import static org.apache.calcite.util.DateTimeStringUtils.getDateFormatter;

/**
 * Translates a {@link RexNode} expression to a Gandiva string.
 */
final class ArrowTranslator {
    private static final Logger logger = LoggerFactory.getLogger(ArrowTranslator.class);

    static List<String> arrowFieldNames(RelDataType rowType) {
        return SqlValidatorUtil.uniquify(rowType.getFieldNames(), SqlValidatorUtil.EXPR_SUGGESTER, true);
    }

    private final RexBuilder rexBuilder;
    private final RelDataType rowType;
    private final List<String> fieldNames;

    private ArrowTranslator(RexBuilder rexBuilder, RelDataType rowType) {
        this.rexBuilder = rexBuilder;
        this.rowType = rowType;
        this.fieldNames = arrowFieldNames(rowType);
    }

    public static ArrowTranslator create(
            RexBuilder rexBuilder,
            RelDataType rowType
    ) {
        return new ArrowTranslator(rexBuilder, rowType);
    }

    List<String> translateMatch(RexNode condition) {
        List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
        if (disjunctions.size() == 1) {
            return translateAnd(disjunctions.get(0));
        } else {
            throw ExceptionUtils.exception(
                    logger,
                    "Unsupported disjunctive condition " + condition
            ).create(UnsupportedOperationException::new);
        }
    }

    /**
     * Returns the value of the literal.
     *
     * @param literal Literal to translate
     *
     * @return The value of the literal in the form of the actual type
     */
    private static Object literalValue(RexLiteral literal) {
        switch (literal.getTypeName()) {
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final SimpleDateFormat dateFormatter =
                        getDateFormatter(ISO_DATETIME_FRACTIONAL_SECOND_FORMAT);
                final Long millis = literal.getValueAs(Long.class);
                return dateFormatter.format(Objects.requireNonNull(millis, "millis"));
            case DATE:
                final DateString dateString = literal.getValueAs(DateString.class);
                return Objects.requireNonNull(dateString, "dateString").toString();
            default:
                return Objects.requireNonNull(literal.getValue3());
        }
    }

    /**
     * Translate a conjunctive predicate to a SQL string.
     *
     * @param condition A conjunctive predicate
     *
     * @return SQL string for the predicate
     */
    private List<String> translateAnd(RexNode condition) {
        List<String> predicates = new ArrayList<>();
        for (RexNode node : RelOptUtil.conjunctions(condition)) {
            if (node.getKind() == SqlKind.SEARCH) {
                final RexNode node2 = RexUtil.expandSearch(rexBuilder, null, node);
                predicates.addAll(translateMatch(node2));
            } else {
                predicates.add(translateMatch2(node));
            }
        }
        return predicates;
    }

    /**
     * Translates a binary or unary relation.
     *
     * @param node A RexNode that always evaluates to a boolean expression.
     *             Currently, this method is only called from translateAnd.
     * @return The translated SQL string for the relation.
     */
    private String translateMatch2(RexNode node) {
        switch (node.getKind()) {
            case EQUALS:
                return translateBinary("equal", "=", (RexCall) node);
            case NOT_EQUALS:
                return translateBinary("not_equal", "<>", (RexCall) node);
            case LESS_THAN:
                return translateBinary("less_than", ">", (RexCall) node);
            case LESS_THAN_OR_EQUAL:
                return translateBinary("less_than_or_equal_to", ">=", (RexCall) node);
            case GREATER_THAN:
                return translateBinary("greater_than", "<", (RexCall) node);
            case GREATER_THAN_OR_EQUAL:
                return translateBinary("greater_than_or_equal_to", "<=", (RexCall) node);
            case IS_NULL:
                return translateUnary("isnull", (RexCall) node);
            case IS_NOT_NULL:
                return translateUnary("isnotnull", (RexCall) node);
            case IS_NOT_TRUE:
                return translateUnary("isnottrue", (RexCall) node);
            case IS_NOT_FALSE:
                return translateUnary("isnotfalse", (RexCall) node);
            case INPUT_REF:
                final RexInputRef inputRef = (RexInputRef) node;
                return fieldNames.get(inputRef.getIndex()) + " istrue";
            case NOT:
                return translateUnary("isfalse", (RexCall) node);
            default:
                throw new UnsupportedOperationException("Unsupported operator " + node);
        }
    }

    /**
     * Translates a call to a binary operator, reversing arguments if
     * necessary.
     */
    private String translateBinary(String op, String rop, RexCall call) {
        final RexNode left = call.operands.get(0);
        final RexNode right = call.operands.get(1);
        @Nullable String expression = translateBinary2(op, left, right);
        if (expression != null) {
            return expression;
        } else {
            expression = translateBinary2(rop, right, left);
            if (expression != null) {
                return expression;
            }
        }

        throw ExceptionUtils.exception(
                logger,
                "Unsupported binary operator " + call
        ).create(UnsupportedOperationException::new);
    }

    /** Translates a call to a binary operator. Returns null on failure. */
    private @Nullable String translateBinary2(String op, RexNode left, RexNode right) {
        if (right.getKind() != SqlKind.LITERAL) {
            return null;
        } else {
            final RexLiteral rightLiteral = (RexLiteral) right;
            switch (left.getKind()) {
                case INPUT_REF:
                    final RexInputRef left1 = (RexInputRef) left;
                    String name = fieldNames.get(left1.getIndex());
                    return translateOp2(op, name, rightLiteral);
                case CAST:
                    // FIXME This will not work in all cases (for example, we ignore string encoding)
                    return translateBinary2(op, ((RexCall) left).operands.get(0), right);
                default:
                    return null;
            }
        }
    }

    /** Combines a field name, operator, and literal to produce a predicate string. */
    private String translateOp2(String op, String name, RexLiteral right) {
        final Object value = literalValue(right);
        String valueString = value.toString();
        final String valueType = getLiteralType(right.getType());

        if (value instanceof String) {
            final RelDataTypeField field = Objects.requireNonNull(
                    rowType.getField(
                            name,
                            true,
                            false
                    ),
                    "field"
            );
            final SqlTypeName typeName = field.getType().getSqlTypeName();
            if (typeName != SqlTypeName.CHAR) {
                valueString = "'" + valueString + "'";
            }
        }

        return name + " " + op + " " + valueString + " " + valueType;
    }

    /** Translates a call to a unary operator. */
    private String translateUnary(String op, RexCall call) {
        final RexNode opNode = call.operands.get(0);
        final @Nullable String expression = translateUnary2(op, opNode);

        if (expression != null) {
            return expression;
        } else {
            throw ExceptionUtils.exception(
                    logger,
                    "Unsupported unary operator " + call
            ).create(UnsupportedOperationException::new);
        }
    }

    /** Translates a call to a unary operator. Returns null on failure. */
    private @Nullable String translateUnary2(String op, RexNode opNode) {
        if (opNode.getKind() == SqlKind.INPUT_REF) {
            final RexInputRef inputRef = (RexInputRef) opNode;
            final String name = fieldNames.get(inputRef.getIndex());
            return translateUnaryOp(op, name);
        } else {
            return null;
        }
    }

    /** Combines a field name and a unary operator to produce a predicate string. */
    private static String translateUnaryOp(String op, String name) {
        return name + " " + op;
    }

    private static String getLiteralType(RelDataType  type) {
        switch (type.getSqlTypeName()) {
            case DECIMAL:
                return "decimal" + "(" + type.getPrecision() + "," + type.getScale() + ")";
            case REAL:
                return "float";
            case DOUBLE:
                return "double";
            case INTEGER:
                return "integer";
            case VARCHAR:
            case CHAR:
                return "string";
            default:
                throw ExceptionUtils.exception(
                        logger,
                        "Unsupported type " + type
                ).create(UnsupportedOperationException::new);
        }
    }
}
