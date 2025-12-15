package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Api;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.server.AllocatorManager;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataStore implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    public static class NodeNotFoundException extends Exception {
        public NodeNotFoundException(String msg) {
            super(msg);
        }
    }

    private final CacheConfig config;
    private final AllocatorManager allocatorManager;
    private final ConcurrentMap<CachePath, DataNode> nodes = new ConcurrentHashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public DataStore(CacheConfig config, AllocatorManager allocatorManager) {
        this.config = config;
        this.allocatorManager = allocatorManager;
    }

    @Override
    public void close() {
        nodes.values().forEach(DataNode::close);
    }

    public Set<CachePath> getCachePaths() {
        return nodes.keySet();
    }

    public boolean containsNode(CachePath path) {
        return nodes.containsKey(path);
    }

    public Optional<DataNode> getNode(CachePath path) {
        return Optional.ofNullable(nodes.get(path));
    }

    public void add(CachePath path, Schema schema, List<ArrowRecordBatch> arbs) {
        final DataNode dataNode = nodes.get(path);
        if (dataNode == null) {
            final CacheConfig.NodeConfig nodeConfig = config.getNode(path);

            synchronized (rwLock.writeLock()) {
                nodes.put(path, new DataNode(path.path(), nodeConfig, allocatorManager, schema, arbs));
            }
        } else {
            dataNode.add(schema, arbs);
        }
    }

    public boolean deleteNode(CachePath path) {
        synchronized (rwLock.writeLock()) {
            final DataNode node = nodes.remove(path);
            if (node == null) {
                return false;
            } else {
                node.close();
                return true;
            }
        }
    }
//
//    public Map<Integer, Set<Integer>> execute(
//            CachePath path,
//            List<Api.Filter<?>> filters
//    ) {
//        if (path.wildcardCount() > 0) {
//            throw new IllegalArgumentException("Cannot execute queries against paths with wildcards: '" + path + "'");
//        } else {
//            synchronized (rwLock.readLock()) {
//                final DataNode node = getNode(path);
//                return node.execute(filters);
//            }
//        }
//    }
//
//    public void execute(
//            CachePath path,
//            List<Api.Filter<?>> filters,
//            FlightProducer.ServerStreamListener listener
//    ) {
//        if (path.wildcardCount() > 0) {
//            throw new IllegalArgumentException("Cannot execute queries against paths with wildcards: '" + path + "'");
//        } else {
//            synchronized (rwLock.readLock()) {
//                final DataNode node = getNode(path);
//                node.execute(filters, listener);
//            }
//        }
//    }
}
