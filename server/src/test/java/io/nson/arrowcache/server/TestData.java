package io.nson.arrowcache.server;

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
import java.util.*;

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

        logger.debug("Schema: {}", schema.toJson());

        return VectorSchemaRoot.create(schema, allocator);
    }

    public static Map<Integer, Map<String, Object>> loadTestData(VectorSchemaRoot vsc, String fileName) throws IOException {

        final Map<Integer, Map<String, Object>> results = new HashMap<>();

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

            logger.info(line);

            final String[] parts = line.split(",");

            final int id = Integer.parseInt(parts[0].trim());
            final String name = parts[1].trim();
            final float age = Float.parseFloat(parts[2].trim());
            final LocalDate date = LocalDate.parse(parts[3].trim(), DateTimeFormatter.ISO_LOCAL_DATE);

            idVector.set(i, id);
            nameVector.set(i, name.getBytes(StandardCharsets.UTF_8));
            ageVector.set(i, age);
            dateVector.set(i, (int) date.toEpochDay());

            final Map<String, Object> row = new HashMap<>();
            row.put("id", id);
            row.put("name", name);
            row.put("age", age);
            row.put("date", (int)date.toEpochDay());
            results.put(id, row);
        }

        vsc.setRowCount(rowCount);

        return results;
    }

    public static final Set<Integer> KEYS1 = Set.of(11, 14, 21);
    public static final Set<Integer> KEYS2 = Set.of(12, 13);
    public static final Set<Integer> KEYS3 = Set.of(14, 22, 23);
    public static final Set<Integer> KEYS4 = Set.of(12, 21);
}
