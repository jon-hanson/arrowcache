package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.TablePath;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SchemaConfigCodecTest {
    @Test
    public void encodeDecodeTest() {
        final SchemaConfig.AllocatorMaxSizeConfig allocatorMaxSizeConfig = new SchemaConfig.AllocatorMaxSizeConfig(
                CacheUtils.megabytes(256),
                Map.of(
                        "flight-server", CacheUtils.gigabytes(8)
                )
        );

        final SchemaConfig schemaConfig = new SchemaConfig(
                allocatorMaxSizeConfig,
                Map.of(
                    TablePath.valueOfConcat("abc/def"),
                    new SchemaConfig.TableConfig("id"),
                    TablePath.valueOfConcat("abc/ghi"),
                    new SchemaConfig.TableConfig("id")
                )
        );

        final String encoded = SchemaConfig.CODEC.encode(schemaConfig);

        //System.out.println(encoded);

        final SchemaConfig schemaConfig2 = SchemaConfig.CODEC.decode(encoded);

        //assertEquals(cacheConfig, cacheConfig2);
    }
}
