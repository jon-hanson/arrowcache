package io.nson.arrowcache.server;

import io.nson.arrowcache.common.utils.FileUtils;
import io.nson.arrowcache.server.cache.DataSchema;
import io.nson.arrowcache.server.calcite.ArrowCacheSchema;
import io.nson.arrowcache.server.calcite.ArrowCacheSchemaFactory;
import io.nson.arrowcache.server.calcite.CalciteDataContext;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

public class TestCalcite {
    private static final Logger logger = LoggerFactory.getLogger(TestCalcite.class);

    private static final String SQL = "SELECT * FROM test.abc WHERE name = 'ghi'";

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        new TestCalcite().test();
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    public void test() throws IOException, ClassNotFoundException, SQLException {
        final RootSchemaConfig schemaConfig = FileUtils.loadFromResource("schemaconfig-test.json", RootSchemaConfig.CODEC);
        try (
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
                        if (statement.execute(SQL)) {
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

    //@Test
    @EnabledOnOs(OS.LINUX)
    public void test2() throws IOException {
        final RootSchemaConfig schemaConfig = FileUtils.loadFromResource("schemaconfig-test.json", RootSchemaConfig.CODEC);
        try (
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

                final SchemaPlus calciteRootSchema = Frameworks.createRootSchema(true);
                calciteRootSchema.add("test", schema);

                final FrameworkConfig frameworkConfig =
                        Frameworks.newConfigBuilder()
                                .parserConfig(SqlParser.Config.DEFAULT.withUnquotedCasing(Casing.UNCHANGED))
                                .defaultSchema(calciteRootSchema)
                                .build();

                final RelNode node = convertSqlToRelationalExpression(frameworkConfig, SQL);

                final CalciteDataContext dataContext = new CalciteDataContext(calciteRootSchema, node);

                try (final Interpreter interpreter = new Interpreter(dataContext, node)) {
                    interpreter.forEach(row -> {
                        final StringBuilder sb = new StringBuilder();
                        for (Object cell : row) {
                            sb.append(cell).append(", ");
                        }
                        logger.info(sb.toString());
                    });
                }
            }
        }
    }

    private static RelNode convertSqlToRelationalExpression(FrameworkConfig frameworkConfig, String sql) {
        logger.debug("Converting the following SQL query to a relational expression:\n{}", sql);

        // Creating planner with default settings (what is a planner?)
        final Planner planner = Frameworks.getPlanner(frameworkConfig);

        RelNode node;
        try {
            final SqlNode parsedSql = planner.parse(sql);
            final SqlNode validatedSql = planner.validate(parsedSql);
            node = planner.rel(validatedSql).rel;

            // Turn on debug logging to see the relational algebra expression in text form.
            logger.debug("Relational algebra expression (converted from SQL):\n{}", RelOptUtil.toString(node));
        } catch (SqlParseException | ValidationException | RelConversionException ex) {
            throw new RuntimeException(ex);
        }

        return node;
    }
}
