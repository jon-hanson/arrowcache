package io.nson.arrowcache.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class CachePath implements Comparable<CachePath> {
    public static final String WILDCARD = "*";
    public static final String SEP = "/";

    public static CachePath valueOf(String path) {
        if (path.isEmpty()) {
            throw new IllegalArgumentException("path is empty");
        } else if (path.startsWith(SEP)) {
            throw new IllegalArgumentException("path must not start with " + SEP);
        } else if (path.endsWith(SEP)) {
            throw new IllegalArgumentException("path must not end with " + SEP);
        } else {
            final String[] parts = path.split(SEP);
            return valueOf(Arrays.asList(parts));
        }
    }

    public static CachePath valueOf(Iterable<String> parts) {
        final List<String> partsList = new ArrayList<>();
        parts.forEach(partsList::add);
        return new CachePath(partsList);
    }

    public static CachePath valueOf(String... parts) {
        return new CachePath(Arrays.asList(parts));
    }

    private final List<String> parts;

    private CachePath(List<String> parts) {
        this.parts = parts;
        for (String part : parts) {
            if (part.isEmpty()) {
                throw new IllegalArgumentException("Path must not contain empty elements");
            }
        }
    }

    @Override
    public String toString() {
        return String.join(SEP, parts);
    }

    @Override
    public boolean equals(Object rhs) {
        if (rhs == null || getClass() != rhs.getClass()) {
            return false;
        } else {
            final CachePath rhsT = (CachePath) rhs;
            return this.parts.equals(rhsT.parts);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.parts);
    }

    @Override
    public int compareTo(CachePath rhs) {
        final int partsLength = Math.min(this.parts.size(), rhs.parts.size());
        for (int i = 0; i < partsLength; i++) {
            final int cmp = this.parts.get(i).compareTo(rhs.parts.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }

        return Integer.compare(parts.size(), rhs.parts.size());
    }

    public String path() {
        return String.join(SEP, parts);
    }

    public List<String> parts() {
        return this.parts;
    }

    public String name(int i) {
        return this.parts.get(i);
    }

    public int wildcardCount() {
        int count = 0;

        for (String part : this.parts) {
            if (part.equals(WILDCARD)) {
                ++count;
            }
        }

        return count;
    }

    public boolean match(CachePath rhs) {
        if (this.parts.equals(rhs.parts)) {
            return true;
        } else if (this.parts.size() != rhs.parts.size()) {
            return false;
        } else {
            for (int i = 0; i < this.parts.size(); i++) {
                final String lhsPart = this.parts.get(i);
                final String rhsPart = rhs.parts.get(i);
                final boolean match = lhsPart.equals(WILDCARD) ||
                        rhsPart.equals(WILDCARD) ||
                        parts.get(i).equals(rhs.parts.get(i));
                if (!match) {
                    return false;
                }
            }
            return true;
        }
    }
}
