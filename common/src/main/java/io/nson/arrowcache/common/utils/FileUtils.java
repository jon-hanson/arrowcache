package io.nson.arrowcache.common.utils;

import java.io.*;
import java.util.List;
import java.util.stream.*;

import static java.util.stream.Collectors.*;

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
