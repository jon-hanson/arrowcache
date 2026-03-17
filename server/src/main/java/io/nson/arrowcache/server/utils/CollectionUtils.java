package io.nson.arrowcache.server.utils;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.SortedSet;

public abstract class CollectionUtils extends io.nson.arrowcache.common.utils.CollectionUtils {
    private CollectionUtils() {}

    public static Range range(int startIndexInc, int endIndexExc) {
        return new Range(startIndexInc, endIndexExc);
    }

    public static class Range {
        // Start is inclusive.
        private final int startIndexInc;

        // End is exclusive.
        private final int endIndexExc;

        public Range(int startIndexInc, int endIndexExc) {
            this.startIndexInc = startIndexInc;
            this.endIndexExc = endIndexExc;
        }

        @Override
        public boolean equals(Object rhs) {
            if (rhs == null || getClass() != rhs.getClass()) {
                return false;
            } else {
                final Range rhsT = (Range) rhs;
                return startIndexInc == rhsT.startIndexInc && endIndexExc == rhsT.endIndexExc;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(startIndexInc, endIndexExc);
        }

        @Override
        public String toString() {
            return "Range[" + startIndexInc + " -> " + endIndexExc + ')';
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
                ranges.add(new Range(start, Math.min(i, size)));
            }
            start = i + 1;
        }

        if (start < size) {
            ranges.add(new Range(start, size));
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

    public static List<Range> generateSlices(int endExc, int sliceSize) {
        assert(endExc >= 0);
        assert(sliceSize > 0);

        final List<Range> ranges = new ArrayList<>();

        int startInc = 0;
        while  (startInc < endExc) {
            final int endExc2 = Math.min(startInc + sliceSize, endExc) ;
            ranges.add(new Range(startInc, endExc2));
            startInc = endExc2;
        }

        return ranges;
    }

    public static OptionalInt ofNullable(@Nullable Integer i) {
        return i == null ? OptionalInt.empty() : OptionalInt.   of(i);
    }
}
