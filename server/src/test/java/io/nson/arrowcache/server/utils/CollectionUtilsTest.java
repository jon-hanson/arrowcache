package io.nson.arrowcache.server.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class CollectionUtilsTest {
    private static SortedSet<Integer> sortedSetOf(int... values) {
        return Arrays.stream(values)
                .boxed()
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static final SortedSet<Integer> ints = sortedSetOf(3, 4, 6, 10, 15);

    @Test
    public void testRangesFromExcluded() {

    }
}
