package io.nson.arrowcache.client;

import io.nson.arrowcache.common.Api;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public interface ClientAPI {
    interface Source<T> {
        boolean hasNext();
        T next();
    }

    interface Listener<T> {
        void onNext(T values);
        void onError(Throwable ex);
        void onCompleted();
    }

    void put(List<String> path, Schema schema, Source<ArrowRecordBatch> src);

    void get(List<String> path, Api.Query query, Listener<ArrowRecordBatch> listener);

    void remove(List<String> path, Api.Query query);
}
