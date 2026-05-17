package io.nson.arrowcache.common.utils;
import io.nson.arrowcache.common.Codec;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public abstract class FileUtils {
    private FileUtils() {}

    public static BufferedReader openResource(String name) throws IOException {
        final InputStream is = FileUtils.class.getClassLoader().getResourceAsStream(name);
        if (is == null) {
            throw new IOException("Failed to open resource '" + name + "'");
        } else {
            return new BufferedReader(new InputStreamReader(is, UTF_8));
        }
    }

    public static InputStream openZippedResource(String name) throws IOException {
        final InputStream is = FileUtils.class.getClassLoader().getResourceAsStream(name);
        if (is == null) {
            throw new IOException("Failed to open resource '" + name + "'");
        } else {
            final ZipInputStream zis = new ZipInputStream(is);
            final ZipEntry zipEntry = zis.getNextEntry();
            if (zipEntry == null) {
                throw new IOException("Zip resource '" + name + "' is empty");
            } else {
                return zis;
            }
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
        return br.lines().onClose(CheckedFunctions.unchecked(br::close));
    }

    public static List<String> openResourceAsLineList(String name) throws IOException {
        final Stream<String> lineStr = openResourceAsLineStream(name);
        final List<String> lines = lineStr.collect(toList());
        lineStr.close();
        return lines;
    }

    public static <T> T loadFromResource(
            String resourceName,
            Codec<T, String> codec
    ) throws IOException {
        return codec.decode(FileUtils.readResource(resourceName));
    }

    public static <T> T loadFile(
            String fileName,
            Codec<T, String> codec
    ) throws IOException {
        try (final InputStream is = new FileInputStream(fileName)) {
            return codec.decode(new String(is.readAllBytes(), Charset.defaultCharset()));
        }
    }
}
