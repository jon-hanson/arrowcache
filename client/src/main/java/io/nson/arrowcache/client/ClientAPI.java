package io.nson.arrowcache.client;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.Set;

public interface ClientAPI extends AutoCloseable {
    interface Source {
        boolean hasNext();
        void loadNext();
    }

    interface Listener {
        void onNext(VectorSchemaRoot vsc);
        void onError(Throwable ex);
        void onCompleted();
    }

    void put(List<String> schemaPath, String table, VectorSchemaRoot vsc, Source src);

    void put(List<String> schemaPath, String table, VectorSchemaRoot vsc);

    void get(List<String> schemaPath, String table, Set<?> keys, Listener listener);

    void remove(List<String> schemaPath, String table, Set<?> keys);

    void mergeTables(List<String> schemaPath);

    void mergeTables(List<String> schemaPath, Set<String> tables);
}
