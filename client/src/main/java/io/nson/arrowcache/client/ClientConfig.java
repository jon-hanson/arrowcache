package io.nson.arrowcache.client;


import io.nson.arrowcache.common.JsonCodec;

public class ClientConfig {
    public static final JsonCodec<ClientConfig> CODEC = new JsonCodec<>(ClientConfig.class) {};

    private final String serverHost;
    private final int serverPort;

    public ClientConfig(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    public String serverHost() {
        return serverHost;
    }

    public int serverPort() {
        return serverPort;
    }
}
