package io.nson.arrowcache.server.cache;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class CachePath implements Comparable<CachePath> {
    public static final String WILDCARD = "*";
    public static final String SEP = "/";

    public static CachePath valueOf(String path) {
        if (path.isEmpty()) {
            throw new IllegalArgumentException("path is empty");
        } else if (!path.startsWith(SEP)) {
            throw new IllegalArgumentException("path must start with " + SEP);
        } else if (path.endsWith(SEP)) {
            throw new IllegalArgumentException("path must not end with " + SEP);
        } else {
            return new CachePath(path);
        }
    }

    public static CachePath valueOf(List<String> parts) {
        return new CachePath(parts);
    }

    private final String path;
    private final List<String> parts;

    private CachePath(String path) {
        this.path = path;
        this.parts = Arrays.asList(path.split(SEP));
    }

    private CachePath(List<String> parts) {
        this.path = String.join(SEP, parts);
        this.parts = parts;
    }

    @Override
    public String toString() {
        return this.path;
    }

    @Override
    public boolean equals(Object rhs) {
        if (rhs == null || getClass() != rhs.getClass()) {
            return false;
        } else {
            final CachePath rhsT = (CachePath) rhs;
            return Objects.equals(this.path, rhsT.path);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.path);
    }

    @Override
    public int compareTo(CachePath rhs) {
        return this.path.compareTo(rhs.path);
    }

    public String path() {
        return path;
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
        if (this.path.equals(rhs.path)) {
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
