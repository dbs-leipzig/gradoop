package org.gradoop.flink.io.impl.rdbms;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants.RdbmsType;
import org.gradoop.flink.io.impl.rdbms.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.rdbms.metadata.RdbmsTableBase;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToNode;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class RdbmsDataImportTest extends GradoopFlinkTestBase {

  private static PostgresWrapper pw;
  private static Connection con;
  private static MetaDataParser metadataParser;
  private static String gdlPath;

  @BeforeClass
  public static void setUp() throws IOException, SQLException {
    pw = new PostgresWrapper();
    pw.start();

    con = DriverManager.getConnection(pw.getConnectionUrl());

    metadataParser = new MetaDataParser(con, RdbmsType.MYSQL_TYPE);
    metadataParser.parse();

    gdlPath = RdbmsDataImportTest.class
        .getResource("/data/rdbms/expected/cycleTest.gdl").getFile();
  }
  
  @Test
  public void dbMetadataTest() throws SQLException, IOException {
    List<RdbmsTableBase> tableBase = metadataParser.getTableBase();

    assertEquals("Wrong table count !", 1, tableBase.size());
  }

  @Test
  public void tablesToNodesTest() throws Exception {
    List<TableToNode> tablesToNodes = metadataParser.getTablesToNodes();
    List<TableToEdge> tablesToEdges = metadataParser.getTablesToEdges();

    assertEquals("Wrong tables to nodes count !", 1, tablesToNodes.size());
    assertEquals("Wrong tables to edges count !", 1, tablesToEdges.size());
  }

  @Test
  public void testConvertRdbmsToGraph() throws Exception {

    // creates embedded, temporary postgresql database
    String url = pw.getConnectionUrl();
    String user = "userName";
    String password = "password";
    String jdbcDriverPath = RdbmsDataImportTest.class
        .getResource("/data/rdbms/jdbcDrivers/postgresql-42.2.2.jar").getFile();
    String jdbcDriverClassName = "org.postgresql.Driver";

    // creates rdbms data import of embedded databse
    DataSource dataSource = new RdbmsDataSource(url, user, password, jdbcDriverPath,
        jdbcDriverClassName, getConfig());

    LogicalGraph tempInput = dataSource.getLogicalGraph();
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlPath);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    DataSet<Vertex> v = tempInput.getVertices().map(new MapFunction<Vertex, Vertex>() {

      /**
       * Serial version id
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Vertex map(Vertex v) throws Exception {
        String newLabel = v.getLabel().split("\\.")[1];
        v.setLabel(newLabel);
        return v;
      }
    });

    LogicalGraph input = getConfig().getLogicalGraphFactory().fromDataSets(v,
        tempInput.getEdges());
   
    collectAndAssertTrue(input.equalsByElementData(expected));
  }
  
  @AfterClass
  public static void stopEmbeddedPostgres() {
    pw.stop();
  }
}
