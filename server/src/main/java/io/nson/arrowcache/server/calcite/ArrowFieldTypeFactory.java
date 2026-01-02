package io.nson.arrowcache.server.calcite;

import io.nson.arrowcache.server.utils.ExceptionUtils;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Arrow field type.
 */
public class ArrowFieldTypeFactory {
    private static final Logger logger = LoggerFactory.getLogger(ArrowFieldTypeFactory.class);

    private ArrowFieldTypeFactory() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static RelDataType toType(ArrowType arrowType, JavaTypeFactory typeFactory) {
        RelDataType sqlType = of(arrowType, typeFactory);
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

    /**
     * Converts an Arrow type to a Calcite RelDataType.
     *
     * @param arrowType the Arrow type to convert
     * @param typeFactory the factory to create the Calcite type
     * @return the corresponding Calcite RelDataType
     */
    private static RelDataType of(ArrowType arrowType, JavaTypeFactory typeFactory) {
        switch (arrowType.getTypeID()) {
            case Int:
                int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
                switch (bitWidth) {
                    case 64:
                        return typeFactory.createSqlType(SqlTypeName.BIGINT);
                    case 32:
                        return typeFactory.createSqlType(SqlTypeName.INTEGER);
                    case 16:
                        return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                    case 8:
                        return typeFactory.createSqlType(SqlTypeName.TINYINT);
                    default:
                        throw ExceptionUtils.logError(
                                logger,
                                IllegalArgumentException::new,
                                "Unsupported Int bit width: " + bitWidth
                        );
                }
            case Bool:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case Utf8:
                return typeFactory.createSqlType(SqlTypeName.VARCHAR);
            case FloatingPoint:
                FloatingPointPrecision precision = ((ArrowType.FloatingPoint) arrowType).getPrecision();
                switch (precision) {
                    case SINGLE:
                        return typeFactory.createSqlType(SqlTypeName.REAL);
                    case DOUBLE:
                        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                    default:
                        throw ExceptionUtils.logError(
                                logger,
                                IllegalArgumentException::new,
                                "Unsupported Floating point precision: " + precision
                        );
                }
            case Date:
                return typeFactory.createSqlType(SqlTypeName.DATE);
            case Decimal:
                return typeFactory.createSqlType(SqlTypeName.DECIMAL,
                        ((ArrowType.Decimal) arrowType).getPrecision(),
                        ((ArrowType.Decimal) arrowType).getScale());
            case Time:
                return typeFactory.createSqlType(SqlTypeName.TIME);
            default:
                throw ExceptionUtils.logError(
                        logger,
                        IllegalArgumentException::new,
                        "Unsupported type: " + arrowType
                );
        }
    }
}
