package io.nson.arrowcache.server.utils;

import java.util.HashSet;
import java.util.Set;

public abstract class CollectionUtils {
    private CollectionUtils() {}

    public static <T> Set<T> intersect(Set<T> lhs, Set<T> rhs) {
        final Set<T> result = new HashSet<T>(lhs);
        result.retainAll(rhs);
        return result;
    }
}
