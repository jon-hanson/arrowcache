package io.nson.arrowcache.server.cache;

public class CachePath {
    public static final String SEP = "/";

    public static CachePath valueOf(String pathStr) {
        return valueOf(pathStr.split(SEP));
    }

    private static CachePath valueOf(String[] parts) {
        CachePath cachePath = new CachePath(parts[parts.length - 1]);

        for (int i = parts.length - 2; i >= 0; --i) {
            cachePath = new CachePath(parts[i], cachePath);
        }

        return cachePath;
    }

    private final String name;
    private final CachePath next;

    public CachePath(String name, CachePath next) {
        this.name = name;
        this.next = next;
    }

    public CachePath(String name) {
        this.name = name;
        this.next = null;
    }

    public String name() {
        return name;
    }

    public CachePath next() {
        return next;
    }
}
