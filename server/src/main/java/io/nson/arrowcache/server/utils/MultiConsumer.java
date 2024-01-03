package io.nson.arrowcache.server.utils;

import java.util.*;

public interface MultiConsumer<T> {
    default void consume(T value) {
        consumeAll(Collections.singletonList(value));
    }

    default void consumeAll(Collection<T> values) {
        values.forEach(this::consume);
    }
}
