package io.nson.arrowcache.server.cache;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CacheConfigCodecTest {
    @Test
    public void encodeDecodeTest() {
        final CacheConfig cacheConfig = new CacheConfig(
                Map.of(
                    CachePath.valueOf("/abc/def"),
                    new CacheConfig.NodeConfig("id"),
                    CachePath.valueOf("/abc/ghi"),
                    new CacheConfig.NodeConfig("id")
                )
        );

        final String encoded = CacheConfigCodec.INSTANCE.encode(cacheConfig);

        System.out.println(encoded);

        final CacheConfig cacheConfig2 = CacheConfigCodec.INSTANCE.decode(encoded);

        assertEquals(cacheConfig, cacheConfig2);
    }
}
