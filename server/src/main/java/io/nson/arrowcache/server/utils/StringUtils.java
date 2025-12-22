package io.nson.arrowcache.server.utils;

import org.jspecify.annotations.Nullable;

public abstract class StringUtils {
    private StringUtils() {}

    public static String trim(String s, String suffix) {
        String trimmed = trimOrNull(s, suffix);
        return trimmed != null ? trimmed : s;
    }

    public static @Nullable String trimOrNull(String s, String suffix) {
        return s.endsWith(suffix) ? s.substring(0, s.length() - suffix.length()) : null;
    }
}
