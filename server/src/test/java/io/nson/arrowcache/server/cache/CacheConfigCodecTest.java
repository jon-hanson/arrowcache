package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.CachePath;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                    CachePath.valueOf("abc/def"),
                    new CacheConfig.NodeConfig("id"),
                    CachePath.valueOf("abc/ghi"),
                    new CacheConfig.NodeConfig("id")
                )
        );

        final String encoded = CacheConfigCodec.INSTANCE.encode(cacheConfig);

        System.out.println(encoded);

        final CacheConfig cacheConfig2 = CacheConfigCodec.INSTANCE.decode(encoded);

        //assertEquals(cacheConfig, cacheConfig2);
    }
}
