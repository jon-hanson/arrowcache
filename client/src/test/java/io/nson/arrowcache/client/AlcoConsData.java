package io.nson.arrowcache.client;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import io.nson.arrowcache.common.utils.FileUtils;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public class AlcoConsData {
    private static final TypeMeta.TypedField<Long, IntVector> ID = TypeMeta.LONG.bind("id");
    private static final TypeMeta.TypedField<String, VarCharVector> ENTITY = TypeMeta.STRING.bind("entity");
    private static final TypeMeta.TypedField<String, VarCharVector> CODE = TypeMeta.STRING.bind("code");
    private static final TypeMeta.TypedField<Integer, IntVector> YEAR = TypeMeta.INTEGER.bind("year");
    private static final TypeMeta.TypedField<Float, Float4Vector> CONSUMPTION = TypeMeta.FLOAT.bind("consumption");
    private static final TypeMeta.TypedField<Float, Float4Vector> GDP = TypeMeta.FLOAT.bind("gdp");
    private static final TypeMeta.TypedField<String, VarCharVector> REGION = TypeMeta.STRING.bind("region");

    public static final Schema SCHEMA = new Schema(
            Arrays.asList(
                    ID.notNullableField(),
                    ENTITY.notNullableField(),
                    CODE.notNullableField(),
                    YEAR.notNullableField(),
                    CONSUMPTION.nullableField(),
                    GDP.nullableField(),
                    REGION.nullableField()
            )
    );

    public static void loadTestDataIntoVsc(VectorSchemaRoot vsc, String name) throws IOException {

        final IntVector idVector = ID.getVector(vsc);
        final VarCharVector entityVector = ENTITY.getVector(vsc);
        final VarCharVector codeVector = CODE.getVector(vsc);
        final IntVector yearVector = YEAR.getVector(vsc);
        final Float4Vector consumptionVector = CONSUMPTION.getVector(vsc);
        final Float4Vector gdpVector = GDP.getVector(vsc);
        final VarCharVector regionVector = REGION.getVector(vsc);

        final List<CsvRecord> csvRecs;

        try (final InputStream is = FileUtils.openZippedResource(name)) {
            final CsvReader<CsvRecord> csvReader = CsvReader.builder().ofCsvRecord(is);
            csvRecs =
                    csvReader.stream()
                            .skip(1)
                            .collect(toList());
        }

        final int n = csvRecs.size();

        idVector.allocateNew(n);
        entityVector.allocateNew(n);
        codeVector.allocateNew(n);
        yearVector.allocateNew(n);
        consumptionVector.allocateNew(n);
        gdpVector.allocateNew(n);
        regionVector.allocateNew(n);

        for (int i = 0; i < n; ++i) {
            final int j = i;
            final CsvRecord csvRec = csvRecs.get(i);

            idVector.setSafe(i, i);
            set(csvRec, 0, s -> s.getBytes(StandardCharsets.UTF_8), v -> entityVector.setSafe(j, v));
            set(csvRec, 1, s -> s.getBytes(StandardCharsets.UTF_8), v -> codeVector.setSafe(j, v));
            set(csvRec, 2, Integer::parseInt, v -> yearVector.setSafe(j, v));
            set(csvRec, 3, Float::parseFloat, v -> consumptionVector.setSafe(j, v));
            set(csvRec, 4, Float::parseFloat, v -> gdpVector.setSafe(j, v));
            set(csvRec, 5, s -> s.getBytes(StandardCharsets.UTF_8), v -> regionVector.setSafe(j, v));
        }

        vsc.setRowCount(n);
    }

    private static <T> void set(CsvRecord csvRec, int i, Function<String, T> parser, Consumer<T> cons) {
        set(csvRec.getField(i), parser, cons);
    }

    private static <T> void set(String s, Function<String, T> parser, Consumer<T> cons) {
        if (s != null && !s.isEmpty()) {
            cons.accept(parser.apply(s));
        }
    }
}
