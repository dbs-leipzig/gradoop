/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.impl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.EdgeDataHandler;
import org.gradoop.storage.api.GraphDataHandler;
import org.gradoop.storage.api.VertexDataHandler;

import java.io.IOException;

/**
 * Manages {@link EPGMStore} instances which can be
 * used to store an EPGM instance in HBase.
 */
public class HBaseEPGMStoreFactory {
  /**
   * Logger
   */
  private static final Logger LOG =
    Logger.getLogger(HBaseEPGMStoreFactory.class);

  /**
   * Private constructor to avoid instantiation.
   */
  private HBaseEPGMStoreFactory() {
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config            cluster configuration
   * @param vertexDataHandler vertex storage handler
   * @param edgeDataHandler   edge storage handler
   * @param graphDataHandler  graph storage handler
   * @param <VD>              vertex data type
   * @param <ED>              edge data type
   * @param <GD>              graph data type
   * @return a graph store instance or {@code null in the case of errors}
   */
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMStore<VD, ED, GD> createOrOpenEPGMStore(
    final Configuration config,
    final VertexDataHandler<VD, ED> vertexDataHandler,
    final EdgeDataHandler<ED, VD> edgeDataHandler,
    final GraphDataHandler<GD> graphDataHandler) {
    return createOrOpenEPGMStore(config, vertexDataHandler, edgeDataHandler,
      graphDataHandler, GConstants.DEFAULT_TABLE_VERTICES,
      GConstants.DEFAULT_TABLE_EDGES, GConstants.DEFAULT_TABLE_GRAPHS);
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config            cluster configuration
   * @param vertexDataHandler vertex storage handler
   * @param edgeDataHandler   edge storage handler
   * @param graphDataHandler  graph storage handler
   * @param prefix            prefix for HBase table name
   * @param <VD>              vertex data type
   * @param <ED>              edge data type
   * @param <GD>              graph data type
   * @return a graph store instance or {@code null in the case of errors}
   */
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMStore<VD, ED, GD> createOrOpenEPGMStore(
    final Configuration config,
    final VertexDataHandler<VD, ED> vertexDataHandler,
    final EdgeDataHandler<ED, VD> edgeDataHandler,
    final GraphDataHandler<GD> graphDataHandler, final String prefix) {
    return createOrOpenEPGMStore(config, vertexDataHandler, edgeDataHandler,
      graphDataHandler, prefix + GConstants.DEFAULT_TABLE_VERTICES,
      prefix + GConstants.DEFAULT_TABLE_EDGES,
      prefix + GConstants.DEFAULT_TABLE_GRAPHS);
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config            cluster configuration
   * @param vertexDataHandler vertex storage handler
   * @param edgeDataHandler   edge storage handler
   * @param graphDataHandler  graph graph storage handler
   * @param vertexTableName   vertex data table name
   * @param edgeTableName     edge data table name
   * @param graphTableName    graph data table name
   * @param <VD>              vertex data type
   * @param <ED>              edge data type
   * @param <GD>              graph data type
   * @return EPGM store instance or {@code null in the case of errors}
   */
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> EPGMStore<VD, ED, GD> createOrOpenEPGMStore(
    final Configuration config,
    final VertexDataHandler<VD, ED> vertexDataHandler,
    final EdgeDataHandler<ED, VD> edgeDataHandler,
    final GraphDataHandler<GD> graphDataHandler, final String vertexTableName,
    final String edgeTableName, final String graphTableName) {
    try {
      createTablesIfNotExists(config, vertexDataHandler, edgeDataHandler,
        graphDataHandler, vertexTableName, edgeTableName, graphTableName);

      HTable vertexDataTable = new HTable(config, vertexTableName);
      HTable edgeDataTable = new HTable(config, edgeTableName);
      HTable graphDataTable = new HTable(config, graphTableName);

      return new HBaseEPGMStore<>(vertexDataTable, edgeDataTable,
        graphDataTable, vertexDataHandler, edgeDataHandler, graphDataHandler);
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
    deleteEPGMStore(config, GConstants.DEFAULT_TABLE_VERTICES,
      GConstants.DEFAULT_TABLE_EDGES, GConstants.DEFAULT_TABLE_GRAPHS);
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
   * @param vertexDataHandler   vertex storage handler
   * @param edgeDataHandler     edge storage handler
   * @param graphDataHandler    graph storage handler
   * @param vertexDataTableName vertex data table name
   * @param edgeTableName       edge data table name
   * @param graphDataTableName  graph data table name
   * @throws IOException
   */
  private static void createTablesIfNotExists(final Configuration config,
    final VertexDataHandler vertexDataHandler,
    final EdgeDataHandler edgeDataHandler,
    final GraphDataHandler graphDataHandler, final String vertexDataTableName,
    final String edgeTableName, final String graphDataTableName) throws
    IOException {
    HTableDescriptor vertexDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(vertexDataTableName));
    HTableDescriptor edgeDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(edgeTableName));
    HTableDescriptor graphDataTableDescriptor =
      new HTableDescriptor(TableName.valueOf(graphDataTableName));

    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(vertexDataTableDescriptor.getName())) {
      vertexDataHandler.createTable(admin, vertexDataTableDescriptor);
    }
    if (!admin.tableExists(edgeDataTableDescriptor.getName())) {
      edgeDataHandler.createTable(admin, edgeDataTableDescriptor);
    }
    if (!admin.tableExists(graphDataTableDescriptor.getName())) {
      graphDataHandler.createTable(admin, graphDataTableDescriptor);
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
    LOG.info("deleting table: " + tableDescriptor.getNameAsString());
    admin.disableTable(tableDescriptor.getName());
    admin.deleteTable(tableDescriptor.getName());
  }
}
