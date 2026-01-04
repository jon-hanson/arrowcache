package io.nson.arrowcache.server;

import java.util.Collections;
import java.util.Map;

public class SchemaConfig {
    protected final Map<String, RootSchemaConfig.ChildSchemaConfig> childSchema;
    protected final Map<String, RootSchemaConfig.TableConfig> tables;

    public SchemaConfig(
            Map<String, RootSchemaConfig.ChildSchemaConfig> childSchema,
            Map<String, RootSchemaConfig.TableConfig> tables
    ) {
        this.childSchema = childSchema;
        this.tables = tables;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "childSchema=" + childSchema +
                "tables=" + tables +
                '}';
    }

    public Map<String, RootSchemaConfig.ChildSchemaConfig> childSchema() {
        return childSchema == null ? Collections.emptyMap() : childSchema;
    }

    public Map<String, RootSchemaConfig.TableConfig> tables() {
        return tables == null ? Collections.emptyMap() : tables;
    }
}
