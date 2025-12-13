package io.nson.arrowcache.server.cache;

import com.google.gson.FormattingStyle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.nson.arrowcache.common.utils.Codec;

import java.io.IOException;

public class CacheConfigCodec implements Codec<CacheConfig, String> {
    public static final CacheConfigCodec INSTANCE = new CacheConfigCodec();

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(CachePath.class, new CachePathTypeAdaptor())
            .setFormattingStyle(FormattingStyle.PRETTY.withIndent("    "))
            .create();

    @Override
    public String encode(CacheConfig raw) {
        return gson.toJson(raw);
    }

    @Override
    public CacheConfig decode(String enc) {
        return gson.fromJson(enc, CacheConfig.class);
    }
}

class CachePathTypeAdaptor extends TypeAdapter<CachePath> {

    @Override
    public void write(JsonWriter jsonWriter, CachePath cachePath) throws IOException {
        jsonWriter.value(cachePath.toString());
    }

    @Override
    public CachePath read(JsonReader jsonReader) throws IOException {
        return CachePath.valueOf(jsonReader.nextString());
    }
}