package io.nson.arrowcache.server.utils;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;

import java.util.List;
import java.util.OptionalInt;

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
}
