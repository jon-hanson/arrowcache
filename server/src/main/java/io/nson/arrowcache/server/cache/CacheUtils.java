package io.nson.arrowcache.server.cache;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class CacheUtils {

    public static int findKeyColumn(Schema schema, String name) {
        final List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i).getName().equals(name)) {
                return i;
            }
        }

        throw new RuntimeException("Key column name '" + name + "' not found in schema");
    }
}
