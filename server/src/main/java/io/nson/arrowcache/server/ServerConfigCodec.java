package io.nson.arrowcache.server;

import com.google.gson.FormattingStyle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nson.arrowcache.common.utils.Codec;

public class ServerConfigCodec implements Codec<ServerConfig, String> {
    public static final ServerConfigCodec INSTANCE = new ServerConfigCodec();

    private static final Gson gson = new GsonBuilder()
            .setFormattingStyle(FormattingStyle.PRETTY.withIndent("    "))
            .create();

    @Override
    public String encode(ServerConfig raw) {
        return gson.toJson(raw);
    }

    @Override
    public ServerConfig decode(String enc) {
        return gson.fromJson(enc, ServerConfig.class);
    }
}

