package io.nson.arrowcache.common.utils;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class CheckedFunctions {
    private CheckedFunctions() {}

    public interface CheckedSupplier<T, EX extends Exception> {
        T get() throws EX;

        default Supplier<T> unchecked() {
            return () -> {
                try {
                    return get();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            };
        }
    }

    public interface CheckedConsumer<T, EX extends Exception> {
        void consume(T t) throws EX;

        default Consumer<T> uncheckedC() {
            return (T t) -> {
                try {
                    consume(t);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            };
        }
    }

    public interface CheckedFunction<T, R, EX extends Exception> {
        R apply(T t) throws EX;

        default Function<T, R> unchecked() {
            return t -> {
                try {
                    return apply(t);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            };
        }
    }

    public interface CheckedRunnable<E extends Exception> {
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

    public static <E extends Exception> Runnable unchecked(CheckedRunnable<E> run) {
        return run.unchecked();
    }
}
