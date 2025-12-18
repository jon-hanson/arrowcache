package io.nson.arrowcache.server.cache;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.common.utils.JsonCodec;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

public final class CacheConfig {
    public static final JsonCodec<CacheConfig> CODEC = new Codec();

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

    private static class Codec extends JsonCodec<CacheConfig> {
        private static class CachePathTypeAdaptor extends TypeAdapter<CachePath> {

            @Override
            public void write(JsonWriter jsonWriter, CachePath cachePath) throws IOException {
                jsonWriter.value(cachePath.toString());
            }

            @Override
            public CachePath read(JsonReader jsonReader) throws IOException {
                return CachePath.valueOf(jsonReader.nextString());
            }
        }

        public Codec() {
            super(CacheConfig.class);
        }

        @Override
        protected GsonBuilder prepare(GsonBuilder gsonBuilder) {
            return gsonBuilder.registerTypeAdapter(CachePath.class, new CachePathTypeAdaptor());
        }
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

    public Optional<NodeConfig> getNode(CachePath path) {
        return nodes.keySet().stream()
                .filter(path::match)
                .min(Comparator.comparing(CachePath::wildcardCount))
                .map(nodes::get);
    }
}
