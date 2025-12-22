package io.nson.arrowcache.server2;

import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class ArrowSchema extends AbstractSchema {
    private static final Logger logger = LoggerFactory.getLogger(ArrowSchema.class);

    private final Map<String, ArrowTable> tableMap;

    ArrowSchema() {
        this.tableMap = new HashMap<>();
    }

}
