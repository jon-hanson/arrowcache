package io.nson.arrowcache.server;

import io.nson.arrowcache.common.utils.JsonCodec;

public class ServerConfig {
    public static final JsonCodec<ServerConfig> CODEC = new JsonCodec<>(ServerConfig.class) {};

    private final int port;

    public ServerConfig(int port) {
        this.port = port;
    }

    public int port() {
        return port;
    }
}
