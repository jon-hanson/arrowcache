package io.nson.arrowcache.client;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.QueryCodecs;
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

    private static final CallOption CALL_TIMEOUT = CallOptions.timeout(1, TimeUnit.HOURS);

    private static final Api.Query QUERY1 = new Api.Query(
            List.of(
                    new Api.MVFilter<String>(
                            "name",
                            Api.MVFilter.Operator.IN,
                            Set.of("abc", "def")
                    )
            )
    );

    private static final Api.Query QUERY2 = new Api.Query(
            List.of(
                    new Api.SVFilter<Float>(
                            "age",
                            Api.SVFilter.Operator.NOT_EQUALS,
                            2.3f
                    )
            )
    );

    private static final Map<Location, FlightClient> locationClientMap = new HashMap<>();

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
            locationClientMap.put(location, client);

            listFlights(client);

            logger.info("Calling startPut");

            final FlightDescriptor FLIGHT_DESC = FlightDescriptor.path("test");

            {
                final FlightClient.ClientStreamListener listener = client.startPut(
                        FLIGHT_DESC,
                        vsc,
                        new AsyncPutListener(),
                        CALL_TIMEOUT
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

            logger.info("ActionTypes:");
            {
                for(ActionType actionType : client.listActions(CALL_TIMEOUT)) {
                    logger.info("    {}", actionType);
                }
            }

            listFlights(client);

            doGetFlight(client, FLIGHT_DESC);

//            {
//                logger.info("QUERY1: {}", QueryCodecs.API_TO_AVRO.encode(QUERY1));
//                final byte[] bytes = QueryCodecs.API_TO_BYTES.encode(QUERY1);
//                final FlightDescriptor FLIGHT_DESC_QUERY = FlightDescriptor.command(bytes);
//
//                doGetFlight(client, FLIGHT_DESC_QUERY);
//            }

            {
                logger.info("QUERY2: {}", QueryCodecs.API_TO_AVRO.encode(QUERY2));
                final byte[] bytes = QueryCodecs.API_TO_BYTES.encode(QUERY2);
                final FlightDescriptor FLIGHT_DESC_QUERY = FlightDescriptor.command(bytes);

                doGetFlight(client, FLIGHT_DESC_QUERY);
            }

            listFlights(client);

            {
                // Do delete action
                final Iterator<Result> deleteActionResult = client.doAction(new Action("DELETE",
                        FLIGHT_DESC.getPath().get(0).getBytes(StandardCharsets.UTF_8)));
                while (deleteActionResult.hasNext()) {
                    Result result = deleteActionResult.next();
                    logger.info("Client (Do Delete Action): {}", ArrowUtils.resultToString(result));
                }
            }

            listFlights(client);

            logger.info("Done");
        } catch (Exception ex) {
            logger.error("Unexpected exception", ex);
        }
    }

    private static void listFlights(FlightClient client) {
        logger.info("Flights:");
        client.listFlights(Criteria.ALL, CALL_TIMEOUT)
                .forEach(flightInfo -> {
                    logger.info("    {}", flightInfo);
                });
    }

    private static void doGetFlight(FlightClient client, FlightDescriptor flightDescriptor) {
        final FlightInfo flightInfo = client.getInfo(flightDescriptor, CALL_TIMEOUT);
        logger.info("FlightInfo: {}", flightInfo);

        flightInfo.getEndpoints().forEach(endPoint -> {
            for (Location loc : endPoint.getLocations()) {
                logger.info("    Location: {}", loc);
                final FlightClient flightClient = locationClientMap.get(loc);

                try (final FlightStream flightStream = flightClient.getStream(endPoint.getTicket(), CALL_TIMEOUT)) {
                    logger.info("    Schema: {}", flightStream.getSchema());

                    final VectorSchemaRoot vsc = flightStream.getRoot();

                    logger.info("    Iterating over flightStream");
                    while (flightStream.next()) {
                        vsc.getFieldVectors()
                                .forEach(fv -> {
                                    final int count = fv.getValueCount();
                                    logger.info("        FieldVector: {} count={}", fv.getName(), count);
                                    for (int i = 0; i < count; ++i) {
                                        logger.info("            {}", fv.getObject(i));
                                    }
                                });
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
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

        logger.info("Schema: {}", schema.toJson());

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
