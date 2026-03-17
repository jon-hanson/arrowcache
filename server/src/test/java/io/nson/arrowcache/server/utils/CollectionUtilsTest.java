package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.server.utils.CollectionUtils.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.nson.arrowcache.server.utils.CollectionUtils.range;

public class CollectionUtilsTest {
    private static SortedSet<Integer> sortedSetOf(int... values) {
        return Arrays.stream(values)
                .boxed()
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static final SortedSet<Integer> INTS = sortedSetOf(3, 4, 6, 10, 15);
    private static final SortedSet<Integer> INTS2 = sortedSetOf(5);

    @Test
    public void testRangesFromExcluded() {
        final List<Range> actual = CollectionUtils.rangesFromExcluded(20, INTS);
        final List<Range> expected = List.of(range(0, 3), range(5, 6), range(7, 10), range(11, 15), range(16, 20));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testRangesFromExcluded2() {
        final List<Range> actual = CollectionUtils.rangesFromExcluded(15, INTS);
        final List<Range> expected = List.of(range(0, 3), range(5, 6), range(7, 10), range(11, 15));
        Assertions.assertIterableEquals(expected, actual);
    }


    @Test
    public void testRangesFromExcluded4() {
        final List<Range> actual = CollectionUtils.rangesFromExcluded( 7, INTS2);
        final List<Range> expected = List.of(range(0, 5), range(6, 7));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testRangesFromExcluded5() {
        final List<Range> actual = CollectionUtils.rangesFromExcluded( 5, INTS2);
        final List<Range> expected = List.of(range(0,  5));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testRangesFromExcluded8() {
        final List<Range> actual = CollectionUtils.rangesFromExcluded(4, INTS2);
        final List<Range> expected = List.of(range(0, 4));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testGenerateSlices1() {
        final List<Range> actual = CollectionUtils.generateSlices(5, 10);
        final List<Range> expected = List.of(range(0, 5));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testGenerateSlices2() {
        final List<Range> actual = CollectionUtils.generateSlices(10, 10);
        final List<Range> expected = List.of(range(0, 10));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testGenerateSlices3() {
        final List<Range> actual = CollectionUtils.generateSlices(15, 10);
        final List<Range> expected = List.of(range(0, 10), range(10, 15));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testGenerateSlices4() {
        final List<Range> actual = CollectionUtils.generateSlices(10, 2);
        final List<Range> expected = List.of(range(0, 2), range(2, 4), range(4, 6), range(6, 8), range(8, 10));
        Assertions.assertIterableEquals(expected, actual);
    }

    @Test
    public void testGenerateSlices5() {
        final List<Range> actual = CollectionUtils.generateSlices(5, 1);
        final List<Range> expected = List.of(range(0, 1), range(1, 2), range(2, 3), range(3, 4), range(4, 5));
        Assertions.assertIterableEquals(expected, actual);
    }
}
