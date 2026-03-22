package io.nson.arrowcache.server.cache;

import java.util.Map;
import java.util.Set;

public interface SchemaHierarchy {
    String name();

    Set<String> tableNames();

    Map<String, ? extends SchemaHierarchy> childSchema() ;
}
