package io.nson.arrowcache.common.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class LocalDateLogicalType extends LogicalType {
    public static final String LOGICAL_TYPE_NAME = "localDate";

    private static class TypeFactory implements LogicalTypes.LogicalTypeFactory {
        private static final LogicalType INSTANCE = new LocalDateLogicalType();

        @Override
        public LogicalType fromSchema(Schema schema) {
            return INSTANCE;
        }

        @Override
        public String getTypeName() {
            return INSTANCE.getName();
        }
    }

    public LocalDateLogicalType() {
        super(LOGICAL_TYPE_NAME);
    }

    @Override
    public void validate(Schema schema) {
        super.validate(schema);
        if (schema.getType() != Schema.Type.BYTES) {
            throw new IllegalArgumentException("Logical type '" + LOGICAL_TYPE_NAME + "' must be BYTES");
        }
    }

    public static void register() {
        LogicalTypes.register(LOGICAL_TYPE_NAME, new TypeFactory());
    }
}

