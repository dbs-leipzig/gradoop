/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.rdbmsimporter;

import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.dataintegration.importer.rdbmsimporter.connection.RdbmsConfig;
import org.gradoop.dataintegration.importer.rdbmsimporter.connection.RdbmsConnection;
import org.gradoop.dataintegration.importer.rdbmsimporter.functions.CreateEdgesOfTables;
import org.gradoop.dataintegration.importer.rdbmsimporter.functions.CreateVertices;
import org.gradoop.dataintegration.importer.rdbmsimporter.functions.DeletePkFkProperties;
import org.gradoop.dataintegration.importer.rdbmsimporter.metadata.MetaDataParser;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.sql.Connection;
import java.sql.SQLException;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.BROADCAST_VARIABLE;

/**
 * A graph data import for relational databases. This data import was tested with
 * mysql,mariadb,postgresql and sql-server management systems. The execution is currently limited to
 * 64 database tables due to limitations of Flink's union operator (max 64 datasets in parallel).
 * ARRAY data type is not supported.
 */
public class RdbmsDataSource implements DataSource {

  /**
   * Gradoop Flink configuration.
   */
  private GradoopFlinkConfig flinkConfig;

  /**
   * Database configuration.
   */
  private RdbmsConfig rdbmsConfig;

  /**
   * Converts a relational database into an EPGM graph.
   * <p>
   * The data source expects a standard JDBC url, e.g. (JDBC:mysql://localhost/employees) and a
   * valid path to a proper JDBC driver.
   *
   * @param url valid JDBC url (e.g. jdbc:mysql://localhost/employees)
   * @param user database user name
   * @param pw database user password
   * @param jdbcDriverPath path to proper JDBC driver
   * @param jdbcDriverClassName valid JDBC driver class name
   * @param flinkConfig gradoop flink configuration
   */
  public RdbmsDataSource(
    String url, String user, String pw, String jdbcDriverPath,
    String jdbcDriverClassName, GradoopFlinkConfig flinkConfig) {
    this.flinkConfig = flinkConfig;
    this.rdbmsConfig = new RdbmsConfig(null, url, user, pw, jdbcDriverPath, jdbcDriverClassName);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    Logger logger = Logger.getLogger(RdbmsDataSource.class);

    DataSet<Vertex> vertices;
    DataSet<Edge> edges;

    Connection con = RdbmsConnection.create().getConnection(rdbmsConfig);
    MetaDataParser metadataParser = null;

    try {
      rdbmsConfig.setRdbmsType(con.getMetaData().getDatabaseProductName().toLowerCase());
      metadataParser = new MetaDataParser(con, rdbmsConfig.getRdbmsType());
      metadataParser.parse();
    } catch (SQLException e) {
      logger.error(e);
    }

    // creates vertices from rdbms table tuples
    vertices = CreateVertices.create(flinkConfig, rdbmsConfig, metadataParser);

    edges =
      CreateEdgesOfTables.create().convert(flinkConfig, rdbmsConfig, metadataParser, vertices);

    // cleans vertices by deleting primary key and foreign key
    // properties
    vertices = vertices
      .map(new DeletePkFkProperties())
      .withBroadcastSet(
        flinkConfig.getExecutionEnvironment().fromCollection(
          metadataParser.getTablesToNodes()), BROADCAST_VARIABLE);

    return flinkConfig.getLogicalGraphFactory().fromDataSets(vertices, edges);
  }

  @Override
  public GraphCollection getGraphCollection() {
    return flinkConfig.getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }
}
