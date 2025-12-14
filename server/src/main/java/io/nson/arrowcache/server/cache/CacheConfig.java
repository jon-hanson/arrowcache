package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.common.utils.FileUtils;

import java.io.IOException;
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

        public String keyName() {
            return keyName;
        }
    }

    public static final class AllocatorMaxSizeConfig {
        private final long defaultSize;
        private final Map<String, Long> allocatorSizeMap;

        public AllocatorMaxSizeConfig(long defaultSize, Map<String, Long> allocatorSizeMap) {
            this.defaultSize = defaultSize;
            this.allocatorSizeMap = allocatorSizeMap;
        }

        public long defaultSize() {
            return defaultSize;
        }

        public Map<String, Long> allocatorSizeMap() {
            return allocatorSizeMap;
        }

        public long getAllocatorMaxSize(String allocatorName) {
            final Long value = allocatorSizeMap.get(allocatorName);
            return value == null ? defaultSize : value;
        }
    }

    public static CacheConfig loadFromResource(String resourceName) throws IOException {
        final String json = FileUtils.readResource(resourceName);
        return CacheConfigCodec.INSTANCE.decode(json);
    }

    private final AllocatorMaxSizeConfig allocatorMaxSizeConfig;
    private final Map<CachePath, NodeConfig> nodes;

    public CacheConfig(
            AllocatorMaxSizeConfig allocatorMaxSizeConfig,
            Map<CachePath, NodeConfig> nodes
    ) {
        this.allocatorMaxSizeConfig = allocatorMaxSizeConfig;
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "allocatorMaxSizeConfig=" + allocatorMaxSizeConfig +
                "nodes=" + nodes +
                '}';
    }

    public AllocatorMaxSizeConfig allocatorMaxSizeConfig() {
        return allocatorMaxSizeConfig;
    }

    public Map<CachePath, NodeConfig> nodes() {
        return nodes;
    }

    public NodeConfig getNode(CachePath path) {
        final CachePath match = nodes.keySet().stream()
                .filter(path::match)
                .min(Comparator.comparing(CachePath::wildcardCount))
                .orElseThrow(() -> new IllegalArgumentException("No nodes found that match path '" + path + "'"));

        return nodes.get(match);
    }
}
