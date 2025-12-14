package io.nson.arrowcache.client;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.utils.FileUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TestData {
    private static final Logger logger = LoggerFactory.getLogger(TestData.class);

    public static VectorSchemaRoot createTestDataVSC(BufferAllocator allocator) {

        final Field idField = new Field(
                "id",
                FieldType.notNullable(new ArrowType.Int(32, true)),
                null
        );

        final Field nameField = new Field(
                "name",
                FieldType.nullable(new ArrowType.Utf8()),
                null
        );

        final Field ageField = new Field(
                "age",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                null
        );

        final Field dateField = new Field(
                "date",
                FieldType.nullable(new ArrowType.Date(DateUnit.DAY)),
                null
        );

        final Schema schema = new Schema(Arrays.asList(idField, nameField, ageField, dateField), null);

        logger.info("Schema: {}", schema.toJson());

        return VectorSchemaRoot.create(schema, allocator);
    }

    public static VectorSchemaRoot loadTestData(VectorSchemaRoot vsc, String fileName) throws IOException {

        final IntVector idVector = (IntVector) vsc.getVector("id");
        final VarCharVector nameVector = (VarCharVector) vsc.getVector("name");
        final Float4Vector ageVector = (Float4Vector) vsc.getVector("age");
        final DateDayVector dateVector = (DateDayVector) vsc.getVector("date");

        final List<String> lines = FileUtils.openResourceAsLineList(fileName);
        final int rowCount = lines.size();

        idVector.allocateNew(rowCount);
        nameVector.allocateNew(rowCount);
        ageVector.allocateNew(rowCount);
        dateVector.allocateNew(rowCount);

        for (int i = 0; i < rowCount; ++i) {
            final String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }

            final String[] parts = line.split(",");

            final int id = Integer.parseInt(parts[0].trim());
            final String name = parts[1].trim();
            final float age = Float.parseFloat(parts[2].trim());
            final LocalDate date = LocalDate.parse(parts[3].trim(), DateTimeFormatter.ISO_LOCAL_DATE);

            idVector.set(i, id);
            nameVector.set(i, name.getBytes(StandardCharsets.UTF_8));
            ageVector.set(i, age);
            dateVector.set(i, (int)date.toEpochDay());
        }

        vsc.setRowCount(rowCount);

        return vsc;
    }

    public static final Api.Query QUERY1 = new Api.Query(
            "",
            List.of(
                    new Api.MVFilter<String>(
                            "name",
                            Api.MVFilter.Operator.IN,
                            Set.of("abc", "def")
                    )
            )
    );

    public static final Api.Query QUERY2 = new Api.Query(
            "",
            List.of(
                    new Api.SVFilter<Float>(
                            "age",
                            Api.SVFilter.Operator.NOT_EQUALS,
                            2.3f
                    )
            )
    );

}
