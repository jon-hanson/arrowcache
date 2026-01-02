package io.nson.arrowcache.server;

import io.nson.arrowcache.common.JsonCodec;

public class ServerConfig {
    public static final JsonCodec<ServerConfig> CODEC = new JsonCodec<>(ServerConfig.class) {};

    private final int serverPort;
    private final int requestLifetimeMins;

    public ServerConfig(int serverPort, int requestLifetimeMins) {
        this.serverPort = serverPort;
        this.requestLifetimeMins = requestLifetimeMins;
    }

    public int serverPort() {
        return serverPort;
    }

    public int requestLifetimeMins() {
        return requestLifetimeMins;
    }
}
