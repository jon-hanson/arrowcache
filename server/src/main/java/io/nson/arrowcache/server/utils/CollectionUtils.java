package io.nson.arrowcache.server.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

public abstract class CollectionUtils extends io.nson.arrowcache.common.utils.CollectionUtils {
    private CollectionUtils() {}

    public static class Range {
        // Start is inclusive.
        private final int startIndexInc;

        // End is exclusive.
        private final int endIndexExc;

        public Range(int startIndexInc, int endIndexExc) {
            this.startIndexInc = startIndexInc;
            this.endIndexExc = endIndexExc;
        }

        public int start() {
            return startIndexInc;
        }

        public int length() {
            return endIndexExc - startIndexInc;
        }
    }

    public static List<Range> rangesFromExcluded(int size, SortedSet<Integer> exclude) {
        final List<Range> ranges = new ArrayList<>();

        int start = 0;

        for (int i : exclude) {
            if (start < i) {
                ranges.add(new Range(start, i));
            }
            start = i + 1;
        }

        if (start < size) {
            ranges.add(new Range(start, size));
        }

        return ranges;
    }

    public static List<Range> rangesFromExcluded(
            int startIndexInc,
            int endIndexExc,
            SortedSet<Integer> exclude
    ) {
        final List<Range> ranges = new ArrayList<>();

        int start = startIndexInc;

        for (int i : exclude) {
            if (start < i && i <= endIndexExc) {
                ranges.add(new Range(start, i));
            }
            start = i + 1;
        }

        if (start < endIndexExc) {
            ranges.add(new Range(start, endIndexExc));
        }

        return ranges;
    }

    public static List<Range> rangesFromIncluded(SortedSet<Integer> include) {
        final List<Range> ranges = new ArrayList<>();

        int start = -1;
        int last = -1;

        for (int i : include) {
            if (start == -1) {
                start = i;
            } else if (i != last + 1) {
                ranges.add(new Range(start, last + 1));
                start = i;
            }
            last = i;
        }

        ranges.add(new Range(start, last + 1));

        return ranges;
    }
}
