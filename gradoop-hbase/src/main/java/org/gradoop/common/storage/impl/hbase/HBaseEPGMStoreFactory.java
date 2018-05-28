/**
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
package org.gradoop.common.storage.impl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.gradoop.common.config.GradoopHBaseConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.EPGMStore;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.VertexHandler;
import org.gradoop.common.util.HBaseConstants;

import java.io.IOException;

/**
 * Manages {@link EPGMStore} instances which can be
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
   *
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return a graph store instance or {@code null in the case of errors}
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  HBaseEPGMStore<G, V, E> createOrOpenEPGMStore(
    final Configuration config,
    final GradoopHBaseConfig<G, V, E> gradoopHBaseConfig,
    final String prefix) {
    return createOrOpenEPGMStore(config,
      GradoopHBaseConfig.createConfig(gradoopHBaseConfig,
        prefix + HBaseConstants.DEFAULT_TABLE_GRAPHS,
        prefix + HBaseConstants.DEFAULT_TABLE_VERTICES,
        prefix + HBaseConstants.DEFAULT_TABLE_EDGES));
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config              Hadoop cluster configuration
   * @param gradoopHBaseConfig  Gradoop configuration
   * @param graphTableName      graph table name
   * @param vertexTableName     vertex table name
   * @param edgeTableName       edge table name
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return a graph store instance or {@code null in the case of errors}
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  HBaseEPGMStore<G, V, E> createOrOpenEPGMStore(
    final Configuration config,
    final GradoopHBaseConfig<G, V, E> gradoopHBaseConfig,
    final String graphTableName,
    final String vertexTableName,
    final String edgeTableName) {
    return createOrOpenEPGMStore(config,
      GradoopHBaseConfig.createConfig(gradoopHBaseConfig,
        graphTableName, vertexTableName, edgeTableName));
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config              Hadoop cluster configuration
   * @param gradoopHBaseConfig  Gradoop HBase configuration
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return EPGM store instance or {@code null in the case of errors}
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  HBaseEPGMStore<G, V, E> createOrOpenEPGMStore(
    final Configuration config,
    final GradoopHBaseConfig<G, V, E> gradoopHBaseConfig) {
    try {
      createTablesIfNotExists(config, gradoopHBaseConfig.getVertexHandler(),
        gradoopHBaseConfig.getEdgeHandler(),
        gradoopHBaseConfig.getGraphHeadHandler(),
        gradoopHBaseConfig.getVertexTableName(),
        gradoopHBaseConfig.getEdgeTableName(),
        gradoopHBaseConfig.getGraphTableName());

      HTable graphDataTable = new HTable(config,
        gradoopHBaseConfig.getGraphTableName());
      HTable vertexDataTable = new HTable(config,
        gradoopHBaseConfig.getVertexTableName());
      HTable edgeDataTable = new HTable(config,
        gradoopHBaseConfig.getEdgeTableName());

      return new HBaseEPGMStore<>(
        graphDataTable, vertexDataTable, edgeDataTable, gradoopHBaseConfig);
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
   * Deletes the graph store based on the given table names.
   *
   * @param config          Hadoop configuration
   * @param vertexTableName vertex data table name
   * @param edgeTableName   edge data table name
   * @param graphTableName  graph data table name
   */
  public static void deleteEPGMStore(final Configuration config,
    final String vertexTableName, final String edgeTableName,
    final String graphTableName) {
    try {
      deleteTablesIfExists(config, vertexTableName, edgeTableName,
        graphTableName);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates the tables used by the graph store.
   *
   * @param config              Hadoop configuration
   * @param vertexHandler   vertex storage handler
   * @param edgeHandler     edge storage handler
   * @param graphHeadHandler    graph storage handler
   * @param vertexDataTableName vertex data table name
   * @param edgeTableName       edge data table name
   * @param graphDataTableName  graph data table name
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @throws IOException
   */
  private static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  void createTablesIfNotExists(final Configuration config,
    final VertexHandler<V, E> vertexHandler,
    final EdgeHandler<E, V> edgeHandler,
    final GraphHeadHandler<G> graphHeadHandler,
    final String vertexDataTableName, final String edgeTableName,
    final String graphDataTableName) throws IOException {

    HTableDescriptor vertexDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(vertexDataTableName));
    HTableDescriptor edgeDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(edgeTableName));
    HTableDescriptor graphDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(graphDataTableName));

    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(vertexDataTableDescriptor.getName())) {
      vertexHandler.createTable(admin, vertexDataTableDescriptor);
    }
    if (!admin.tableExists(edgeDataTableDescriptor.getName())) {
      edgeHandler.createTable(admin, edgeDataTableDescriptor);
    }
    if (!admin.tableExists(graphDataTableDescriptor.getName())) {
      graphHeadHandler.createTable(admin, graphDataTableDescriptor);
    }

    admin.close();
  }

  /**
   * Deletes the tables given tables.
   *
   * @param config              cluster configuration
   * @param vertexDataTableName vertex data table name
   * @param edgeDataTableName   edge data table name
   * @param graphDataTableName  graph data table name
   * @throws IOException
   */
  private static void deleteTablesIfExists(final Configuration config,
    final String vertexDataTableName, final String edgeDataTableName,
    final String graphDataTableName) throws IOException {

    HTableDescriptor vertexDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(vertexDataTableName));
    HTableDescriptor edgeDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(edgeDataTableName));
    HTableDescriptor graphsTableDescriptor =
      new HTableDescriptor(TableName.valueOf(graphDataTableName));

    HBaseAdmin admin = new HBaseAdmin(config);

    if (admin.tableExists(vertexDataTableDescriptor.getName())) {
      deleteTable(admin, vertexDataTableDescriptor);
    }
    if (admin.tableExists(edgeDataTableDescriptor.getName())) {
      deleteTable(admin, edgeDataTableDescriptor);
    }
    if (admin.tableExists(graphsTableDescriptor.getName())) {
      deleteTable(admin, graphsTableDescriptor);
    }

    admin.close();
  }

  /**
   * Deletes a HBase table.
   *
   * @param admin           HBase admin
   * @param tableDescriptor descriptor for the table to delete
   * @throws IOException
   */
  private static void deleteTable(final HBaseAdmin admin,
    final HTableDescriptor tableDescriptor) throws IOException {
    admin.disableTable(tableDescriptor.getName());
    admin.deleteTable(tableDescriptor.getName());
  }
}
