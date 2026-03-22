package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.server.cache.SchemaHierarchy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArrowServerUtilsTest {

    private static final class SchemaHierarchyImpl implements SchemaHierarchy {

        private final String name;
        private final Set<String> tableNames;
        private final Map<String, ? extends SchemaHierarchy> childSchema;

        private SchemaHierarchyImpl(
                String name,
                Set<String> tableNames,
                Map<String, ? extends SchemaHierarchy> childSchema
        ) {
            this.name = name;
            this.tableNames = tableNames;
            this.childSchema = childSchema;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Set<String> tableNames() {
            return tableNames;
        }

        @Override
        public Map<String, ? extends SchemaHierarchy> childSchema() {
            return childSchema;
        }
    }

    private static final SchemaHierarchy SIMPLE_SCHEMA = new SchemaHierarchyImpl(
        "aaa",
            Set.of("yyy", "zzz"),
            Map.of(
                    "bbb", new SchemaHierarchyImpl(
                            "bbb",
                            Set.of("uuu", "vvv"),
                            Map.of(
                                    "ccc", new SchemaHierarchyImpl(
                                            "ccc",
                                            Set.of("www"),
                                            Map.of()
                                    )
                            )
                    ), "ccc", new SchemaHierarchyImpl(
                            "ccc",
                            Set.of("xxx"),
                            Map.of()
                    )
            )
    );

    private static Set<List<String>> SIMPLE_TABLE_PATHS = Set.of(
            List.of("aaa", "bbb"),
            List.of("aaa", "bbb", "ccc"),
            List.of("aaa", "ccc"),
            List.of("aaa")
    );

    @Test
    public void getSchemaPathsTest() {
        final Set<List<String>> tablePaths =  ArrowServerUtils.getSchemaPaths(SIMPLE_SCHEMA);
        assertEquals(SIMPLE_TABLE_PATHS, tablePaths);
    }
}
