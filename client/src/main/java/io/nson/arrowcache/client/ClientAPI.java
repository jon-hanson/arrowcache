package io.nson.arrowcache.client;

import org.apache.arrow.vector.VectorSchemaRoot;

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

    void put(String schema, String table, VectorSchemaRoot vsc, Source src);

    void put(String schema, String table, VectorSchemaRoot vsc);

    void get(String schema, String table, Set<?> keys, Listener listener);

    void remove(String schema, String table, Set<?> keys);
}
