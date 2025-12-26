package io.nson.arrowcache.common.utils;

import com.google.gson.FormattingStyle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.Strictness;

public class JsonCodec<T> implements Codec<T, String> {

    private final Class<T> clazz;

    private final Gson gson;

    public JsonCodec(Class<T> clazz, Gson gson) {
        this.clazz = clazz;
        this.gson = gson;
    }

    public JsonCodec(Class<T> clazz) {
        this.clazz = clazz;
        this.gson = prepare(
                new GsonBuilder()
                        .setFormattingStyle(FormattingStyle.PRETTY.withIndent("    "))
                        .setStrictness(Strictness.STRICT)
        ).create();
    }

    protected GsonBuilder prepare(GsonBuilder gsonBuilder) {
        return gsonBuilder;
    }

    @Override
    public String encode(T raw) {
        return gson.toJson(raw);
    }

    @Override
    public T decode(String enc) {
        return gson.fromJson(enc, clazz);
    }
}
