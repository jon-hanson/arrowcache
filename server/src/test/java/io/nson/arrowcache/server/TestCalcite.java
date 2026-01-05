package io.nson.arrowcache.server;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.cache.DataSchema;
import io.nson.arrowcache.server.calcite.ArrowCacheSchema;
import io.nson.arrowcache.server.calcite.ArrowCacheSchemaFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

public class TestCalcite {
    private static final Logger logger = LoggerFactory.getLogger(TestCalcite.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        new TestCalcite().test();
    }

    @Test
    public void test() throws IOException, ClassNotFoundException, SQLException {
        final RootSchemaConfig schemaConfig = FileUtils.loadFromResource("schemaconfig-test.json", RootSchemaConfig.CODEC);
        try(
                final RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
                final DataSchema rootSchema = new DataSchema(rootAllocator, schemaConfig.name(), schemaConfig)
        ) {
            ArrowCacheSchemaFactory.initialise(rootAllocator, rootSchema);

            try (final VectorSchemaRoot vsc = TestData.createTestDataVSC(rootAllocator)) {
                final ArrowCacheSchema schema = ArrowCacheSchemaFactory.instance().rootSchema();
                final VectorUnloader unloader = new VectorUnloader(vsc);

                logger.info("Loading testdata1.csv");
                final Map<Integer, Map<String, Object>> testDataMap = TestData.loadTestData(vsc, "testdata1.csv");
                schema.addBatches("abc", vsc.getSchema(), unloader.getRecordBatch());

                logger.info("Loading testdata2.csv");
                testDataMap.putAll(TestData.loadTestData(vsc, "testdata2.csv"));
                schema.addBatches("abc", vsc.getSchema(), unloader.getRecordBatch());

                Class.forName("org.apache.calcite.jdbc.Driver");
                final Properties props = new Properties();
                props.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

                try (final Connection connection = DriverManager.getConnection("jdbc:calcite:model=src/test/resources/model-test.json", props)) {
                    final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
                    try (final Statement statement = calciteConnection.createStatement()) {
                        if (statement.execute("SELECT * FROM test.abc WHERE name = 'ghi'")) {
                            final ResultSet rs = statement.getResultSet();
                            final ResultSetMetaData rsMetadata = rs.getMetaData();

                            final StringBuilder sb = new StringBuilder();
                            for (int i = 1; i <= rsMetadata.getColumnCount(); i++) {
                                sb.append(rsMetadata.getColumnName(i)).append(", ");
                            }
                            sb.append("\n");

                            while (rs.next()) {
                                for (int i = 1; i <= rsMetadata.getColumnCount(); i++) {
                                    sb.append(rs.getObject(i)).append(", ");
                                }
                                sb.append("\n");
                            }

                            logger.info("********: {}", sb);
                        }
                    }
                }
            }

            ArrowCacheSchemaFactory.shutdown();
        }
    }
}
