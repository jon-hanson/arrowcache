package io.nson.arrowcache.server2;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.TestData;
import io.nson.arrowcache.server2.calcite.ArrowSchema;
import io.nson.arrowcache.server2.calcite.ArrowSchemaFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

public class TestCalcite {
    private static final Logger logger = LoggerFactory.getLogger(TestCalcite.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        final SchemaConfig schemaConfig = FileUtils.loadFromResource("cacheconfig.json", SchemaConfig.CODEC);
        try(
                final AllocatorManager allocatorManager = new AllocatorManager(schemaConfig.allocatorMaxSizeConfig());
                final BufferAllocator bufferAlloc = allocatorManager.newChildAllocator("test-data");
                final VectorSchemaRoot vsc = TestData.createTestDataVSC(bufferAlloc);
                final ArrowSchema schema = ArrowSchemaFactory.rootSchema();
        ) {
            final VectorUnloader unloader = new VectorUnloader(vsc);

            logger.info("Loading testdata1.csv");
            final Map<Integer, Map<String, Object>> testDataMap = TestData.loadTestData(vsc, "testdata1.csv");
            schema.addBatches("abc", vsc.getSchema(), unloader.getRecordBatch());

//            logger.info("Loading testdata2.csv");
//            testDataMap.putAll(TestData.loadTestData(vsc, "testdata2.csv"));
//            schema.addBatches("ghi", vsc.getSchema(), unloader.getRecordBatch());

            Class.forName("org.apache.calcite.jdbc.Driver");
            final Properties props = new Properties();
            props.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            try (final Connection connection = DriverManager.getConnection("jdbc:calcite:model=src/main/resources/model.json", props)) {
                final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
                try (final Statement statement = calciteConnection.createStatement()) {
                    if (statement.execute("SELECT * FROM test.abc")) {
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
                    }
                }
            }
        }
    }
}
