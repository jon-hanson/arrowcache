package io.nson.arrowcache.server.cache;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CacheUtils {
    private static final Logger logger = LoggerFactory.getLogger(CacheUtils.class);

    private CacheUtils() {}

    public static void validateSchema(Schema schema, String keyName) {
        if (schema.findField(keyName) == null) {
            logger.error("Key column name '{}' not found in schema", keyName);
            throw new RuntimeException("Key column name '" + keyName + "' not found in schema");
        }
    }

    public static int findKeyColumn(Schema schema, String keyName) {
        final List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i).getName().equals(keyName)) {
                return i;
            }
        }

        logger.error("Key column name '{}' not found in schema", keyName);
        throw new RuntimeException("Key column name '" + keyName + "' not found in schema");
    }

    public static long megabytes(long n) {
        return n * 1024 * 1024;
    }

    public static long gigabytes(long n) {
        return n * 1024 * 1024 * 1024;
    }
}
