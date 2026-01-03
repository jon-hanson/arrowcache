package io.nson.arrowcache.server.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.util.ImmutableIntList;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Built-in methods in the Arrow adapter.
 *
 * @see org.apache.calcite.util.BuiltInMethod
 */
//@SuppressWarnings("ImmutableEnumChecker")
enum ArrowMethod {
    ARROW_QUERY(ArrowCacheTable.class, "query", DataContext.class,
            ImmutableIntList.class, List.class);

    final Method method;

    static final ImmutableMap<Method, ArrowMethod> MAP;

    static {
        final ImmutableMap.Builder<Method, ArrowMethod> builder =
                ImmutableMap.builder();
        for (ArrowMethod value : ArrowMethod.values()) {
            builder.put(value.method, value);
        }
        MAP = builder.build();
    }

    ArrowMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }
}
