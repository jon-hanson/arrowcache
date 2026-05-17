package io.nson.arrowcache.client;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.time.LocalDate;
import java.time.LocalDateTime;

public final class TypeMeta {
    private TypeMeta() {}

    public interface TypedField<JT, VV extends ValueVector> {
        TypeDescriptor<JT, VV> typeDescriptor();

        String name();

        default VV getVector(VectorSchemaRoot vsc) {
            return (VV)vsc.getVector(name());
        }

        default Field nullableField() {
            return new Field(name(), FieldType.nullable(typeDescriptor().arrowType()), null);
        }

        default Field notNullableField() {
            return new Field(name(), FieldType.notNullable(typeDescriptor().arrowType()), null);
        }
    }

    public static class TypedFieldImpl<JT, VV extends ValueVector> implements TypedField<JT, VV> {
        private final TypeDescriptor<JT, VV> typeDescriptor;
        private final String name;

        public TypedFieldImpl(TypeDescriptor<JT, VV> typeDescriptor, String name) {
            this.typeDescriptor = typeDescriptor;
            this.name = name;
        }

        @Override
        public TypeDescriptor<JT, VV> typeDescriptor() {
            return typeDescriptor;
        }

        @Override
        public String name() {
            return name;
        }
    }

    public interface TypeDescriptor<JT, VV extends ValueVector> {
        Class<JT> javaType();

        ArrowType arrowType();

        default TypedField<JT, VV> bind(String name) {
            return new TypedFieldImpl<>(this, name);
        }
    }

    public static final class TypeDescriptorImpl<JT, VV extends ValueVector>  implements TypeDescriptor<JT, VV> {

        private final Class<JT> javaType;
        private final ArrowType arrowType;

        public TypeDescriptorImpl(
                Class<JT> javaType,
                ArrowType arrowType
        ) {
            this.javaType = javaType;
            this.arrowType = arrowType;
        }

        @Override
        public Class<JT> javaType() {
            return javaType;
        }

        @Override
        public ArrowType arrowType() {
            return arrowType;
        }
    }

    public static final TypeDescriptor<Void, NullVector> NULL = new TypeDescriptorImpl<>(Void.class, ArrowType.Null.INSTANCE);
    public static final TypeDescriptor<Boolean, BitVector> BOOLEAN = new TypeDescriptorImpl<>(Boolean.class, ArrowType.Bool.INSTANCE);
    public static final TypeDescriptor<Integer, IntVector> INTEGER = new TypeDescriptorImpl<>(Integer.class, new ArrowType.Int(32, false));
    public static final TypeDescriptor<Long, IntVector> LONG = new TypeDescriptorImpl<>(Long.class, new ArrowType.Int(64, false));
    public static final TypeDescriptor<Float, Float4Vector> FLOAT = new TypeDescriptorImpl<>(Float.class, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
    public static final TypeDescriptor<Double, Float8Vector> DOUBLE = new TypeDescriptorImpl<>(Double.class, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    public static final TypeDescriptor<String, VarCharVector> STRING = new TypeDescriptorImpl<>(String.class, ArrowType.Utf8.INSTANCE);
    public static final TypeDescriptor<LocalDate, DateDayVector> LOCALDATE = new TypeDescriptorImpl<>(LocalDate.class, new ArrowType.Date(DateUnit.DAY));
    public static final TypeDescriptor<LocalDateTime, DateMilliVector> LOCALDATETIME = new TypeDescriptorImpl<>(LocalDateTime.class, new ArrowType.Date(DateUnit.MILLISECOND));
}
