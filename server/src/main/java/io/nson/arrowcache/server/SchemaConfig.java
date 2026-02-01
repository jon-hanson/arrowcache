package io.nson.arrowcache.server;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

@NullMarked
public class SchemaConfig {
    protected final @Nullable Map<String, RootSchemaConfig.ChildSchemaConfig> childSchema;
    protected final @Nullable Map<String, RootSchemaConfig.TableConfig> tables;

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
