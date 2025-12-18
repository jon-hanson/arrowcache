package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.CachePath;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class CacheConfigCodecTest {
    @Test
    public void encodeDecodeTest() {
        final CacheConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig = new CacheConfig.AllocatorMaxSizeConfig(
                CacheUtils.megabytes(256),
                Map.of(
                        "flight-server", CacheUtils.gigabytes(8)
                )
        );

        final CacheConfig cacheConfig = new CacheConfig(
                allocatorMaxSizeConfig,
                Map.of(
                    CachePath.valueOfConcat("abc/def"),
                    new CacheConfig.NodeConfig("id"),
                    CachePath.valueOfConcat("abc/ghi"),
                    new CacheConfig.NodeConfig("id")
                )
        );

        final String encoded = CacheConfig.CODEC.encode(cacheConfig);

        //System.out.println(encoded);

        final CacheConfig cacheConfig2 = CacheConfig.CODEC.decode(encoded);

        //assertEquals(cacheConfig, cacheConfig2);
    }
}
