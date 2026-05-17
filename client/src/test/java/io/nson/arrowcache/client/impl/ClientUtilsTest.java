package io.nson.arrowcache.client.impl;

import io.nson.arrowcache.client.ClientAPI;
import io.nson.arrowcache.client.impl.ArrowFlightClientImpl.SchemaDescriptorImpl;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClientUtilsTest {

    private static List<List<String>> SIMPLE_TABLE_PATHS = List.of(
            List.of("aaa", "bbb", "uuu"),
            List.of("aaa", "bbb", "vvv"),
            List.of("aaa", "bbb", "ccc", "www"),
            List.of("aaa", "ccc", "xxx"),
            List.of("aaa", "yyy"),
            List.of("aaa", "zzz")
    );

    private static ClientAPI.SchemaDescriptor SIMPLE_SCHEMA_DESC =
            new SchemaDescriptorImpl(
                    "aaa",
                    Set.of("yyy", "zzz"),
                    Map.of(
                            "bbb", new SchemaDescriptorImpl(
                                    "bbb",
                                    Set.of("uuu", "vvv"),
                                    Map.of(
                                            "ccc",  new SchemaDescriptorImpl(
                                                    "ccc",
                                                    Set.of("www"),
                                                    Map.of()
                                            )
                                    )
                            ), "ccc", new SchemaDescriptorImpl(
                                    "ccc",
                                    Set.of("xxx"),
                                    Map.of()
                            )
                    )
            );

    @Test
    public void testSplice() {
        final ClientAPI.SchemaDescriptor actualDesc = ClientUtils.splice(SIMPLE_TABLE_PATHS);
        //System.out.println(actualDesc);

        assertEquals(SIMPLE_SCHEMA_DESC, actualDesc, "Spliced schema");
    }
}
