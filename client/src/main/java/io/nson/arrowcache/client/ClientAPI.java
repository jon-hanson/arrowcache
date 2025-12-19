package io.nson.arrowcache.client;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.CachePath;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

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

    void put(CachePath path, VectorSchemaRoot vsc, Source src);

    void put(CachePath path, VectorSchemaRoot vsc);

    void get(CachePath path, List<Model.Filter<?>> filters, Listener listener);

    void remove(CachePath path, List<Model.Filter<?>> filters);
}
