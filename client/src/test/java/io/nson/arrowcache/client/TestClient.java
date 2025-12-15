package io.nson.arrowcache.client;

import io.nson.arrowcache.common.utils.ArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestClient {
    private static final Logger logger = LoggerFactory.getLogger(TestClient.class);

    private static final CallOption CALL_TIMEOUT = CallOptions.timeout(1, TimeUnit.HOURS);

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
                final VectorSchemaRoot vsc = TestData.createTestDataVSC(allocator);
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

                TestData.loadTestDataIntoVsc(vsc, "testdata1.csv");

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

            {
//                logger.info("QUERY1: {}", QueryCodecs.API_TO_AVRO.encode(TestData.QUERY1));
//                final byte[] bytes = QueryCodecs.API_TO_BYTES.encode(TestData.QUERY1);
//                final FlightDescriptor FLIGHT_DESC_QUERY = FlightDescriptor.command(bytes);
//
//                doGetFlight(client, FLIGHT_DESC_QUERY);
            }

            {
//                logger.info("QUERY2: {}", QueryCodecs.API_TO_AVRO.encode(TestData.QUERY2));
//                final byte[] bytes = QueryCodecs.API_TO_BYTES.encode(TestData.QUERY2);
//                final FlightDescriptor FLIGHT_DESC_QUERY = FlightDescriptor.command(bytes);
//
//                doGetFlight(client, FLIGHT_DESC_QUERY);
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
}
