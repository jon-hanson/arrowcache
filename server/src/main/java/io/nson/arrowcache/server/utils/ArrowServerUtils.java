package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.server.cache.DataSchema;
import io.nson.arrowcache.server.cache.SchemaHierarchy;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class ArrowServerUtils {
    private ArrowServerUtils() {}

    public static final Schema EMPTY_SCHEMA = new Schema(List.of(), null);

    public static CallStatus exception(CallStatus callStatus, Logger logger, String msg) {
        logger.error(msg);
        return callStatus.withDescription(msg);
    }

    public static OptionalInt findKeyColumn(Schema schema, String keyColumnName) {
        final List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i).getName().equals(keyColumnName)) {
                return OptionalInt.of(i);
            }
        }

        return OptionalInt.empty();
    }

    public static Set<List<String>> getSchemaPaths(SchemaHierarchy schema) {
        return getSchemaPaths(schema, new ArrayList<>(), new HashSet<>(), false);
    }

    private static Set<List<String>> getSchemaPaths(
            SchemaHierarchy schema,
            List<String> path,
            Set<List<String>> acc,
            boolean addTables
    ) {
        path.add(schema.name());

        if (addTables) {
            schema.tableNames().forEach(tableName -> {
                path.add(tableName);
                acc.add(new ArrayList<>(path));
                path.remove(path.size() - 1);
            });
        } else {
            acc.add(new ArrayList<>(path));
        }

        schema.childSchema().forEach((name, childSchema) -> {
            getSchemaPaths(childSchema, path, acc, addTables);
        });

        path.remove(path.size() - 1);

        return acc;
    }

}
