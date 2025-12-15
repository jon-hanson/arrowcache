package io.nson.arrowcache.common.utils;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class Functors {
    private Functors() {}

    public static <S, T> List<T> listMap(List<S> l, Function<S, T> f) {
        return l.stream().map(f).collect(toList());
    }

    public static <S, T> Set<T> setMap(Collection<S> l, Function<S, T> f) {
        return l.stream().map(f).collect(toSet());
    }
}
