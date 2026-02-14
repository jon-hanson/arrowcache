package io.nson.arrowcache.server.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

public abstract class CollectionUtils extends io.nson.arrowcache.common.utils.CollectionUtils {
    private CollectionUtils() {}

    public static class Slice {
        // Start is inclusive.
        private final int start;

        // End is exclusive.
        private final int end;

        public Slice(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int start() {
            return start;
        }

        public int length() {
            return end - start;
        }
    }

    public static List<Slice> slicesFromExcluded(int size, SortedSet<Integer> exclude) {
        final List<Slice> slices = new ArrayList<>();

        int start = 0;

        for (int i : exclude) {
            if (start < i) {
                slices.add(new Slice(start, i));
            }
            start = i + 1;
        }

        if (start < size) {
            slices.add(new Slice(start, size));
        }

        return slices;
    }

    public static List<Slice> slicesFromIncluded(SortedSet<Integer> include) {
        final List<Slice> slices = new ArrayList<>();

        int start = -1;
        int last = -1;

        for (int i : include) {
            if (start == -1) {
                start = i;
            } else if (i != last + 1) {
                slices.add(new Slice(start, last + 1));
                start = i;
            }
            last = i;
        }

        slices.add(new Slice(start, last + 1));

        return slices;
    }
}
