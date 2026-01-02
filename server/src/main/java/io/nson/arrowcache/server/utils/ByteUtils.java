package io.nson.arrowcache.server.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

public abstract class ByteUtils {
    private ByteUtils() {}

    private static final int UUID_BYTES = Long.BYTES * 2;

    public static UUID asUuid(byte[] bytes) {
        final ByteBuffer bb = ByteBuffer.wrap(bytes);
        final long firstLong = bb.getLong();
        final long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong);
    }

    public static byte[] asBytes(UUID uuid) {
        final ByteBuffer bb = ByteBuffer.allocate(UUID_BYTES);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}
