package io.nson.arrowcache.server;

public class ServerConfig {
    private final int port;

    public ServerConfig(int port) {
        this.port = port;
    }

    public int port() {
        return port;
    }
}
