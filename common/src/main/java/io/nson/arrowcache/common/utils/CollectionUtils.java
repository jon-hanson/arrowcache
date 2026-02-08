package io.nson.arrowcache.common.utils;

import java.util.HashSet;
import java.util.Set;

public class CollectionUtils {
    protected CollectionUtils() {}

    public static <T> Set<T> intersect(Set<T> lhs, Set<T> rhs) {
        final Set<T> result = new HashSet<T>(lhs);
        result.retainAll(rhs);
        return result;
    }
}
