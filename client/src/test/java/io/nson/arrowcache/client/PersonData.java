package io.nson.arrowcache.client;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.common.utils.StringUtils;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;

public class PersonData {

    private static final TypeMeta.TypedField<Long, IntVector> ID = TypeMeta.LONG.bind("id");
    private static final TypeMeta.TypedField<String, VarCharVector> NAME = TypeMeta.STRING.bind("name");
    private static final TypeMeta.TypedField<Float, Float4Vector> AGE = TypeMeta.FLOAT.bind("age");
    private static final TypeMeta.TypedField<LocalDate, DateDayVector> DATE = TypeMeta.LOCALDATE.bind("date");

    public static final Schema SCHEMA =  new Schema(
            List.of(
                    ID.notNullableField(),
                    NAME.nullableField(),
                    AGE.nullableField(),
                    DATE.nullableField()
            )
    );

    public static void loadTestDataIntoVsc(VectorSchemaRoot vsc, String fileName) throws IOException {

        final IntVector idVector = ID.getVector(vsc);
        final VarCharVector nameVector = NAME.getVector(vsc);
        final Float4Vector ageVector = AGE.getVector(vsc);
        final DateDayVector dateVector = DATE.getVector(vsc);

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

            final List<String> parts = StringUtils.split(line, ',');

            final int id = Integer.parseInt(parts.get(0).trim());
            final String name = parts.get(1).trim();
            final float age = Float.parseFloat(parts.get(2).trim());
            final LocalDate date = LocalDate.parse(parts.get(3).trim(), DateTimeFormatter.ISO_LOCAL_DATE);

            idVector.set(i, id);
            nameVector.set(i, name.getBytes(StandardCharsets.UTF_8));
            ageVector.set(i, age);
            dateVector.set(i, (int)date.toEpochDay());
        }

        vsc.setRowCount(rowCount);
    }

    public static final Set<Integer> KEYS1 = Set.of(11, 14, 21);
    public static final Set<Integer> KEYS2 = Set.of(12, 13);
    public static final Set<Integer> KEYS3 = Set.of(14, 22, 23);
    public static final Set<Integer> KEYS4 = Set.of(12, 21);
}
