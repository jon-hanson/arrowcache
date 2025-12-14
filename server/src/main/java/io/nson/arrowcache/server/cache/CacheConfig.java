package io.nson.arrowcache.server.cache;

import java.util.*;

public final class CacheConfig {
    public static final class NodeConfig {
        private final String keyName;

        public NodeConfig(String keyName) {
            this.keyName = keyName;
        }

        @Override
        public String toString() {
            return "NodeConfig{" +
                    "keyName='" + keyName + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object rhs) {
            if (rhs == null || getClass() != rhs.getClass()) {
                return false;
            } else {
                final NodeConfig rhsT = (NodeConfig) rhs;
                return Objects.equals(keyName, rhsT.keyName);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(keyName);
        }

        public String keyName() {
            return keyName;
        }
    }

    private final Map<io.nson.arrowcache.common.CachePath, NodeConfig> nodes;

    public CacheConfig(Map<io.nson.arrowcache.common.CachePath, NodeConfig> nodes) {
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "nodes=" + nodes +
                '}';
    }

    @Override
    public boolean equals(Object rhs) {
        if (rhs == null || getClass() != rhs.getClass()) {
            return false;
        }  else {
            final CacheConfig rhsT = (CacheConfig) rhs;
            return Objects.equals(nodes, rhsT.nodes);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nodes);
    }

    public NodeConfig getNode(io.nson.arrowcache.common.CachePath path) {
        final io.nson.arrowcache.common.CachePath match = nodes.keySet().stream()
                .filter(path::match)
                .min(Comparator.comparing(io.nson.arrowcache.common.CachePath::wildcardCount))
                .orElseThrow(() -> new IllegalArgumentException("No nodes found that match path '" + path + "'"));

        return nodes.get(match);
    }
}
