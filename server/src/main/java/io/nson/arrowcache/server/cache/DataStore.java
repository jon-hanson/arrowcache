package io.nson.arrowcache.server.cache;

import io.nson.arrowcache.common.Model;
import io.nson.arrowcache.common.TablePath;
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

    private final SchemaConfig config;
    private final AllocatorManager allocatorManager;
    private final ConcurrentMap<TablePath, DataNode> nodes = new ConcurrentHashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public DataStore(SchemaConfig config, AllocatorManager allocatorManager) {
        this.config = config;
        this.allocatorManager = allocatorManager;

        logger.debug("Created DataStore with config {}", config);
    }

    @Override
    public void close() {
        logger.info("Closing {} nodes...", nodes.size());
        nodes.values().forEach(DataNode::close);
    }

    public Set<TablePath> getCachePaths() {
        return nodes.keySet();
    }

    public DataNode getNode(TablePath tablePath) {
        return getNodeOpt(tablePath)
                .orElseThrow(() ->
                        ArrowServerUtils.notFound(logger, "No data node found for path " + tablePath)
                );
    }

    public Optional<DataNode> getNodeOpt(TablePath path) {
        return Optional.ofNullable(nodes.get(path));
    }

    public void add(TablePath path, Schema schema, List<ArrowRecordBatch> arbs) {
        logger.info("Adding data node for path {} with {} ArrowRecordBatches", path, arbs.size());
        final DataNode dataNode = nodes.get(path);
        if (dataNode == null) {
            final SchemaConfig.TableConfig tableConfig = config.getNode(path)
                    .orElseThrow(() -> new IllegalArgumentException("No node (or node config) found for path " + path));

            CacheUtils.validateSchema(schema, tableConfig.keyColumn());

            synchronized (rwLock.writeLock()) {
                nodes.put(path, new DataNode(path.path(), tableConfig, allocatorManager, schema, arbs));
            }
        } else {
            dataNode.add(schema, arbs);
        }
    }

    public boolean deleteNode(TablePath tablePath) {
        logger.info("Deleting data node for path {}", tablePath);
        synchronized (rwLock.writeLock()) {
            final DataNode node = nodes.remove(tablePath);
            if (node == null) {
                return false;
            } else {
                node.close();
                return true;
            }
        }
    }

    public void deleteEntries(TablePath tablePath, List<Model.Filter<?>> filters) {
        logger.info("Deleting entries for path {}", tablePath);
        final DataNode node = getNode(tablePath);
        synchronized (rwLock.writeLock()) {
            final Map<Integer, Set<Integer>> batchMatches = node.execute(filters);
            node.deleteEntries(batchMatches);
        }
    }
}
