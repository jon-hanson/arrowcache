package io.nson.arrowcache.client.impl;

import io.nson.arrowcache.client.ClientAPI;
import io.nson.arrowcache.client.impl.ArrowFlightClientImpl.SchemaDescriptorImpl;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Objects;

public abstract class ClientUtils {
    private ClientUtils() {}

    public static ClientAPI.SchemaDescriptor splice(List<List<String>> tablePaths) {
        if (tablePaths.isEmpty() ) {
            throw new IllegalArgumentException("tablePaths must not be empty");
        } else {
            SchemaDescriptorImpl rootScemaDesc = null;

            for (List<String> tablePath : tablePaths) {
                if (tablePath.size() < 2) {
                    throw new IllegalArgumentException("Table path must have at least 2 elements");
                } else {
                    if (rootScemaDesc == null) {
                        rootScemaDesc = new SchemaDescriptorImpl(tablePath.get(0));
                    } else if (!rootScemaDesc.name().equals(tablePath.get(0))) {
                        throw new IllegalArgumentException("All table paths must have the same root name (path element 0)");
                    }
                    merge(rootScemaDesc, tablePath, 1);
                }
            }

            return Objects.requireNonNull(rootScemaDesc);
        }
    }

    private static void merge(
            SchemaDescriptorImpl parentSchemaDesc,
            List<String> tablePath,
            int i
    ) {
        final String name = tablePath.get(i);

        if (i == tablePath.size() - 1) {
            parentSchemaDesc.tables().add(name);
        } else {
            final SchemaDescriptorImpl child =
                    (SchemaDescriptorImpl)parentSchemaDesc.childSchema()
                            .computeIfAbsent(name, SchemaDescriptorImpl::new);
            merge(child, tablePath, i + 1);
        }
    }
}
