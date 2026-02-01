package io.nson.arrowcache.server;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.nson.arrowcache.common.JsonCodec;
import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.time.Duration;

@NullMarked
public class ServerConfig {
    public static final JsonCodec<ServerConfig> CODEC = new JsonCodec<>(ServerConfig.class) {
        @Override
        protected GsonBuilder prepare(GsonBuilder gsonBuilder) {
            return super.prepare(gsonBuilder)
                    .registerTypeAdapter(Duration.class, new TypeAdapter<Duration>() {

                        @Override
                        public void write(JsonWriter out, Duration value) throws IOException {
                            out.value(value.toString());
                        }

                        @Override
                        public Duration read(JsonReader in) throws IOException {
                            return Duration.parse(in.nextString());
                        }
                    });
        }
    };

    private final int serverPort;
    private final Duration requestLifetime;

    public ServerConfig(int serverPort, Duration requestLifetime) {
        this.serverPort = serverPort;
        this.requestLifetime = requestLifetime;
    }

    public int serverPort() {
        return serverPort;
    }

    public Duration requestLifetime() {
        return requestLifetime;
    }
}
