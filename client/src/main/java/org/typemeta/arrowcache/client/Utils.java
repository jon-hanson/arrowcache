package org.typemeta.arrowcache.client;

import java.io.*;
import java.util.List;
import java.util.stream.*;

import static java.util.stream.Collectors.*;

public abstract class Utils {
    private Utils() {}

    public static BufferedReader openResource(String name) throws IOException {
        final InputStream is = Utils.class.getClassLoader().getResourceAsStream(name);
        if (is == null) {
            throw new IOException("Failed to open resource '" + name + "'");
        }
        return new BufferedReader(new InputStreamReader(is));
    }

    public static Stream<String> openResourceAsLineStream(String name) throws IOException {
        final BufferedReader br = openResource(name);
        return br.lines().onClose(wrapException(br::close));
    }

    public static List<String> openResourceAsLineList(String name) throws IOException {
        final Stream<String> lineStr =  openResourceAsLineStream(name);
        final List<String> lines = lineStr.collect(toList());
        lineStr.close();
        return lines;
    }

    interface RunnableChecked<E extends Exception> {
        void run() throws E;

        default Runnable unchecked() {
            return () -> {
                try {
                    run();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            };
        }
    }

    private static <E extends Exception> Runnable wrapException(RunnableChecked<E> run) {
        return run.unchecked();
    }
}
