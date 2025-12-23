package io.nson.arrowcache.client;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.TablePath;
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

    void put(TablePath path, VectorSchemaRoot vsc, Source src);

    void put(TablePath path, VectorSchemaRoot vsc);

    void get(TablePath path, List<Model.Filter<?>> filters, Listener listener);

    void remove(TablePath path, List<Model.Filter<?>> filters);
}
