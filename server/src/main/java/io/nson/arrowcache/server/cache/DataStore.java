package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.CachePath;
import io.nson.arrowcache.server.AllocatorManager;
import io.nson.arrowcache.server.utils.ArrowServerUtils;
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

    private final CacheConfig config;
    private final AllocatorManager allocatorManager;
    private final ConcurrentMap<CachePath, DataNode> nodes = new ConcurrentHashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public DataStore(CacheConfig config, AllocatorManager allocatorManager) {
        this.config = config;
        this.allocatorManager = allocatorManager;

        logger.debug("Created DataStore with config {}", config);
    }

    @Override
    public void close() {
        logger.info("Closing {} nodes...", nodes.size());
        nodes.values().forEach(DataNode::close);
    }

    public Set<CachePath> getCachePaths() {
        return nodes.keySet();
    }

    public DataNode getNode(CachePath cachePath) {
        return getNodeOpt(cachePath)
                .orElseThrow(() ->
                        ArrowServerUtils.notFound(logger, "No data node found for path " + cachePath)
                );
    }

    public Optional<DataNode> getNodeOpt(CachePath path) {
        return Optional.ofNullable(nodes.get(path));
    }

    public void add(CachePath path, Schema schema, List<ArrowRecordBatch> arbs) {
        logger.info("Adding data node for path {} with {} ArrowRecordBatches", path, arbs.size());
        final DataNode dataNode = nodes.get(path);
        if (dataNode == null) {
            final CacheConfig.NodeConfig nodeConfig = config.getNode(path)
                    .orElseThrow(() -> new IllegalArgumentException("No node (or node config) found for path " + path));

            CacheUtils.validateSchema(schema, nodeConfig.keyName());

            synchronized (rwLock.writeLock()) {
                nodes.put(path, new DataNode(path.path(), nodeConfig, allocatorManager, schema, arbs));
            }
        } else {
            dataNode.add(schema, arbs);
        }
    }

    public boolean deleteNode(CachePath cachePath) {
        logger.info("Deleting data node for path {}", cachePath);
        synchronized (rwLock.writeLock()) {
            final DataNode node = nodes.remove(cachePath);
            if (node == null) {
                return false;
            } else {
                node.close();
                return true;
            }
        }
    }

    public void deleteEntries(CachePath cachePath, List<Model.Filter<?>> filters) {
        logger.info("Deleting entries for path {}", cachePath);
        final DataNode node = getNode(cachePath);
        synchronized (rwLock.writeLock()) {
            final Map<Integer, Set<Integer>> batchMatches = node.execute(filters);
            node.deleteEntries(batchMatches);
        }
    }
}
