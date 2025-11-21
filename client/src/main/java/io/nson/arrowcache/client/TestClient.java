package io.nson.arrowcache.client;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.*;
import org.slf4j.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TestClient {
    private static final Logger logger = LoggerFactory.getLogger(TestClient.class);

    private static final CallOption TIMEOUT_CA = CallOptions.timeout(10, TimeUnit.MINUTES);

    /*
    Run with
        --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
        --add-opens=java.base/java.nio=org.apache.arrow.flight.core,ALL-UNNAMED
     */
    public static void main(String[] args) {

        final Location location = Location.forGrpcInsecure("localhost", 12233);

        try(
                final BufferAllocator allocator = new RootAllocator();
                final VectorSchemaRoot vsc = createTestDataVSC(allocator);
                final FlightClient client = FlightClient.builder(allocator, location).build()
        ) {
            logger.info("FlightInfos:");
            client.listFlights(Criteria.ALL, TIMEOUT_CA)
                    .forEach(flightInfo -> {
                        logger.info("    {}", flightInfo);
                    });

            logger.info("Calling startPut");

            final FlightDescriptor flightDesc = FlightDescriptor.path("test");

            {
                final FlightClient.ClientStreamListener listener = client.startPut(
                        flightDesc,
                        vsc,
                        new AsyncPutListener(),
                        TIMEOUT_CA
                );

                loadTestData(vsc, "testdata.csv");

                logger.info("VectorSchemaRoot:");
                logger.info(vsc.contentToTSVString());

                logger.info("Calling listener.putNext");
                listener.putNext();

                logger.info("Calling listener.completed");
                listener.completed();

                logger.info("Calling listener.getResult");
                listener.getResult();
            }

            logger.info("FlightInfos:");
            client.listFlights(Criteria.ALL, TIMEOUT_CA)
                    .forEach(flightInfo -> {
                        logger.info("    {}", flightInfo);
                    });

            final FlightInfo flightInfo = client.getInfo(flightDesc, TIMEOUT_CA);
            logger.info("FlightInfo: {}", flightInfo);

            flightInfo.getEndpoints().forEach(endPoint -> {
                for (Location loc : endPoint.getLocations()) {
                    logger.info("    Location: {}", loc);
                    final FlightClient flightClient = loc.equals(location) ? client : null;
                    try (final FlightStream flightStream = flightClient.getStream(endPoint.getTicket(), TIMEOUT_CA)) {
                        logger.info("    Schema: {}", flightStream.getSchema());

                        final VectorSchemaRoot vsc2 = flightStream.getRoot();

                        logger.info("    Iterating over flightStream");
                        while (flightStream.next()) {
                            vsc2.getFieldVectors()
                                    .forEach(fv -> {
                                        final int count = fv.getValueCount();
                                        logger.info("        FieldVector: {} count={}", fv.getName(), count);
                                        for (int i = 0; i < count; ++i) {
                                            logger.info("    {}", fv.getObject(i));
                                        }
                                    });
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });


            logger.info("Done");
        } catch (Exception ex) {
            logger.error("Unexpected exception", ex);
        }
    }

    private static VectorSchemaRoot createTestDataVSC(BufferAllocator allocator) {

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

        return VectorSchemaRoot.create(schema, allocator);
    }

    private static VectorSchemaRoot loadTestData(VectorSchemaRoot vsc, String fileName) throws IOException {

        final IntVector idVector = (IntVector) vsc.getVector("id");
        final VarCharVector nameVector = (VarCharVector) vsc.getVector("name");
        final Float4Vector ageVector = (Float4Vector) vsc.getVector("age");
        final DateDayVector dateVector = (DateDayVector) vsc.getVector("date");

        final List<String> lines = Utils.openResourceAsLineList(fileName);
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
}
