package io.nson.arrowcache.server;

import io.nson.arrowcache.server.cache.DataNode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DataStore {
    private final ConcurrentMap<String, DataNode> nodes = new ConcurrentHashMap<>();


}
