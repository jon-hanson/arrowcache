package io.nson.arrowcache.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public abstract class FileUtils {
    private FileUtils() {}

    public static BufferedReader openResource(String name) throws IOException {
        final InputStream is = FileUtils.class.getClassLoader().getResourceAsStream(name);
        if (is == null) {
            throw new IOException("Failed to open resource '" + name + "'");
        } else {
            return new BufferedReader(new InputStreamReader(is));
        }
    }

    public static String readResource(String name) throws IOException {
        try (final InputStream is = FileUtils.class.getClassLoader().getResourceAsStream(name)) {
            if (is == null) {
                throw new IOException("Failed to open resource '" + name + "'");
            } else {
                return new String(is.readAllBytes(), Charset.defaultCharset());
            }
        }
    }

    public static Stream<String> openResourceAsLineStream(String name) throws IOException {
        final BufferedReader br = openResource(name);
        return br.lines().onClose(Exceptions.unchecked(br::close));
    }

    public static List<String> openResourceAsLineList(String name) throws IOException {
        final Stream<String> lineStr =  openResourceAsLineStream(name);
        final List<String> lines = lineStr.collect(toList());
        lineStr.close();
        return lines;
    }
}
