package io.nson.arrowcache.common.utils;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class Functors {
    private Functors() {}

    public static <S, T> List<T> listMap(Collection<S> l, Function<S, T> f) {
        if (l.size() == 1) {
            return List.of(f.apply(l.iterator().next()));
        } else {
            return l.stream().map(f).collect(toList());
        }
    }

    public static <S, T> Set<T> setMap(Collection<S> l, Function<S, T> f) {
        if (l.size() == 1) {
            return Set.of(f.apply(l.iterator().next()));
        }  else {
            return l.stream().map(f).collect(toSet());
        }
    }
}
