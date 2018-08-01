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
package org.gradoop.storage.impl.hbase.factory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.gradoop.storage.common.api.EPGMGraphInput;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.api.EdgeHandler;
import org.gradoop.storage.impl.hbase.api.GraphHeadHandler;
import org.gradoop.storage.impl.hbase.api.VertexHandler;
import org.gradoop.storage.impl.hbase.constants.HBaseConstants;

import java.io.IOException;

/**
 * Manages {@link EPGMGraphInput} instances which can be
 * used to store an EPGM instance in HBase.
 */
public class HBaseEPGMStoreFactory {

  /**
   * Private constructor to avoid instantiation.
   */
  private HBaseEPGMStoreFactory() {
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config        Hadoop cluster configuration
   * @param gradoopHBaseConfig Gradoop configuration
   * @param prefix        prefix for HBase table name
   * @return a graph store instance or {@code null in the case of errors}
   */
  public static HBaseEPGMStore createOrOpenEPGMStore(
    final Configuration config,
    final GradoopHBaseConfig gradoopHBaseConfig,
    final String prefix
  ) {
    return createOrOpenEPGMStore(config,
      GradoopHBaseConfig.createConfig(
        gradoopHBaseConfig,
        prefix + HBaseConstants.DEFAULT_TABLE_GRAPHS,
        prefix + HBaseConstants.DEFAULT_TABLE_VERTICES,
        prefix + HBaseConstants.DEFAULT_TABLE_EDGES
      )
    );
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config              Hadoop cluster configuration
   * @param gradoopHBaseConfig  Gradoop HBase configuration
   *
   * @return EPGM store instance or {@code null in the case of errors}
   */
  public static HBaseEPGMStore createOrOpenEPGMStore(
    final Configuration config,
    final GradoopHBaseConfig gradoopHBaseConfig
  ) {
    try {
      Connection connection = ConnectionFactory.createConnection(config);

      createTablesIfNotExists(
        connection.getAdmin(),
        gradoopHBaseConfig.getVertexHandler(),
        gradoopHBaseConfig.getEdgeHandler(),
        gradoopHBaseConfig.getGraphHeadHandler(),
        gradoopHBaseConfig.getVertexTableName(),
        gradoopHBaseConfig.getEdgeTableName(),
        gradoopHBaseConfig.getGraphTableName()
      );

      Table graphDataTable = connection.getTable(gradoopHBaseConfig.getGraphTableName());
      Table vertexDataTable = connection.getTable(gradoopHBaseConfig.getVertexTableName());
      Table edgeDataTable = connection.getTable(gradoopHBaseConfig.getEdgeTableName());

      return new HBaseEPGMStore(
        graphDataTable,
        vertexDataTable,
        edgeDataTable,
        gradoopHBaseConfig,
        connection.getAdmin()
      );
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Deletes the default graph store.
   *
   * @param config Hadoop configuration
   */
  public static void deleteEPGMStore(final Configuration config) {
    deleteEPGMStore(config, HBaseConstants.DEFAULT_TABLE_VERTICES,
      HBaseConstants.DEFAULT_TABLE_EDGES, HBaseConstants.DEFAULT_TABLE_GRAPHS);
  }

  /**
   * Deletes the graph store which table names have the specified prefix.
   *
   * @param config Hadoop configuration
   * @param prefix table prefix
   */
  public static void deleteEPGMStore(final Configuration config, String prefix) {
    deleteEPGMStore(
      config,
      prefix + HBaseConstants.DEFAULT_TABLE_VERTICES,
      prefix + HBaseConstants.DEFAULT_TABLE_EDGES,
      prefix + HBaseConstants.DEFAULT_TABLE_GRAPHS
    );
  }

  /**
   * Deletes the graph store based on the given table names.
   *
   * @param config          Hadoop configuration
   * @param vertexTableName vertex data table name
   * @param edgeTableName   edge data table name
   * @param graphTableName  graph data table name
   */
  private static void deleteEPGMStore(
    final Configuration config,
    final String vertexTableName,
    final String edgeTableName,
    final String graphTableName
  ) {
    try {
      Connection connection = ConnectionFactory.createConnection(config);

      deleteTablesIfExists(
        connection.getAdmin(),
        TableName.valueOf(vertexTableName),
        TableName.valueOf(edgeTableName),
        TableName.valueOf(graphTableName)
      );
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates the tables used by the graph store.
   *
   * @param admin HBase admin instance
   * @param vertexHandler   vertex storage handler
   * @param edgeHandler     edge storage handler
   * @param graphHeadHandler graph storage handler
   * @param vertexTableName vertex data table name
   * @param edgeTableName edge data table name
   * @param graphTableName  graph data table name
   * @throws IOException if checking for the existence of the tables fails
   */
  private static void createTablesIfNotExists(
    final Admin admin,
    final VertexHandler vertexHandler,
    final EdgeHandler edgeHandler,
    final GraphHeadHandler graphHeadHandler,
    final TableName vertexTableName,
    final TableName edgeTableName,
    final TableName graphTableName
  ) throws IOException {

    if (!admin.tableExists(vertexTableName)) {
      vertexHandler.createTable(admin, new HTableDescriptor(vertexTableName));
    }
    if (!admin.tableExists(edgeTableName)) {
      edgeHandler.createTable(admin, new HTableDescriptor(edgeTableName));
    }
    if (!admin.tableExists(graphTableName)) {
      graphHeadHandler.createTable(admin, new HTableDescriptor(graphTableName));
    }

    admin.close();
  }

  /**
   * Deletes the tables given tables.
   *
   * @param admin               HBase admin instance
   * @param vertexDataTableName vertex data table name
   * @param edgeDataTableName   edge data table name
   * @param graphDataTableName  graph data table name
   * @throws IOException if checking for the existence of the tables fails
   */
  private static void deleteTablesIfExists(
    final Admin admin,
    final TableName vertexDataTableName,
    final TableName edgeDataTableName,
    final TableName graphDataTableName
  ) throws IOException {

    if (admin.tableExists(vertexDataTableName)) {
      deleteTable(admin, vertexDataTableName);
    }
    if (admin.tableExists(edgeDataTableName)) {
      deleteTable(admin, edgeDataTableName);
    }
    if (admin.tableExists(graphDataTableName)) {
      deleteTable(admin, graphDataTableName);
    }

    admin.close();
  }

  /**
   * Deletes a HBase table.
   *
   * @param admin HBase admin
   * @param tableName name of the table to delete
   */
  private static void deleteTable(final Admin admin, final TableName tableName) throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }
}
