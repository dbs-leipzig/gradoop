/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.dataintegration.importer.impl.rdbms;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.impl.rdbms.constants.RdbmsConstants.RdbmsType;
import org.gradoop.dataintegration.importer.impl.rdbms.metadata.MetaDataParser;
import org.gradoop.dataintegration.importer.impl.rdbms.metadata.RdbmsTableBase;
import org.gradoop.dataintegration.importer.impl.rdbms.metadata.TableToEdge;
import org.gradoop.dataintegration.importer.impl.rdbms.metadata.TableToVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
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
  private static MetaDataParser metadataParser;
  private static String gdlPath;

  @BeforeClass
  public static void setUp() throws IOException, SQLException {
    pw = new PostgresWrapper();
    pw.start();

    Connection con = DriverManager.getConnection(pw.getConnectionUrl());

    metadataParser = new MetaDataParser(con, RdbmsType.MYSQL_TYPE);
    metadataParser.parse();

    System.out.println(RdbmsDataImportTest.class
      .getResource("/data/rdbms/expected/cycleTest.gdl").getFile());

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
    List<TableToVertex> tablesToNodes = metadataParser.getTablesToVertices();
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
    DataSource dataSource = new RdbmsImporter(url, user, password, jdbcDriverPath,
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
