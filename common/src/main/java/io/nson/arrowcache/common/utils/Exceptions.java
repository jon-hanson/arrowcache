package io.nson.arrowcache.common.utils;

import java.util.function.*;

public abstract class Exceptions {
    private Exceptions() {}

    public interface SideEffect {
        void apply();
    }

    public interface CheckedSideEffect<EX extends Exception> {
        void apply() throws EX;
    }

    public static <EX extends Exception> SideEffect uncheckedSideEffect(CheckedSideEffect<EX> f) {
        return () -> {
            try {
                f.apply();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    public interface CheckedSupplier<T, EX extends Exception> {
        T get() throws EX;
    }

    public static <T, EX extends Exception> Supplier<T> uncheckedSupplier(CheckedSupplier<T, EX> f) {
        return () -> {
            try {
                return f.get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    public interface CheckedConsumer<T, EX extends Exception> {
        void consume(T t) throws EX;
    }

    public static <T, EX extends Exception> Consumer<T> uncheckedConsumer(CheckedConsumer<T, EX> f) {
        return (T t) -> {
            try {
                f.consume(t);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    public interface CheckedFunction<T, R, EX extends Exception> {
        R apply(T t) throws EX;
    }

    public static <T, R, EX extends Exception> Function<T, R> uncheckedFunction(CheckedFunction<T, R, EX> f) {
        return t -> {
            try {
                return f.apply(t);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}
