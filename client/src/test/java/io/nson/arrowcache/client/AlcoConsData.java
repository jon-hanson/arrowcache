package io.nson.arrowcache.client;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import io.nson.arrowcache.common.utils.FileUtils;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
    public static final Schema SCHEMA = new Schema(
            Arrays.asList(
                    new Field(
                            "id",
                            FieldType.notNullable(new ArrowType.Int(32, true)),
                            null
                    ), new Field(
                            "entity",
                            FieldType.notNullable(new ArrowType.Utf8()),
                            null
                    ), new Field(
                            "code",
                            FieldType.notNullable(new ArrowType.Utf8()),
                            null
                    ), new Field(
                            "year",
                            FieldType.notNullable(new ArrowType.Int(16, false)),
                            null
                    ), new Field(
                            "consumption",
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                            null
                    ), new Field(
                            "gdp",
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                            null
                    ), new Field(
                            "region",
                            FieldType.nullable(new ArrowType.Utf8()),
                            null
                    )
            )
    );

    public static void loadTestDataIntoVsc(VectorSchemaRoot vsc, String name) throws IOException {

        final IntVector idVector = (IntVector) vsc.getVector("id");
        final VarCharVector entityVector = (VarCharVector) vsc.getVector("entity");
        final VarCharVector codeVector = (VarCharVector) vsc.getVector("code");
        final UInt2Vector yearVector = (UInt2Vector) vsc.getVector("year");
        final Float4Vector consumptionVector = (Float4Vector) vsc.getVector("consumption");
        final Float4Vector gdpVector = (Float4Vector) vsc.getVector("gdp");
        final VarCharVector regionVector = (VarCharVector) vsc.getVector("region");

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
