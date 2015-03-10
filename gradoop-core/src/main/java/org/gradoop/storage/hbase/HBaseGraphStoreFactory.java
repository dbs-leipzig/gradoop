package org.gradoop.storage.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.storage.GraphStore;

import java.io.IOException;

/**
 * Manages {@link org.gradoop.storage.GraphStore} instances which can be used to
 * store an EPG instance in HBase.
 */
public class HBaseGraphStoreFactory {
  /**
   * Logger
   */
  private static final Logger LOG =
    Logger.getLogger(HBaseGraphStoreFactory.class);

  /**
   * Private constructor to avoid instantiation.
   */
  private HBaseGraphStoreFactory() {
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config        cluster configuration
   * @param vertexHandler vertex storage handler
   * @param graphHandler  graph storage handler
   * @return a graph store instance or {@code null in the case of errors}
   */
  public static GraphStore createOrOpenGraphStore(final Configuration config,
    final VertexHandler vertexHandler, final GraphHandler graphHandler) {
    return createOrOpenGraphStore(config, vertexHandler, graphHandler,
      GConstants.DEFAULT_TABLE_VERTICES, GConstants.DEFAULT_TABLE_GRAPHS);
  }

  /**
   * Creates a graph store or opens an existing one based on the given
   * parameters. If something goes wrong, {@code null} is returned.
   *
   * @param config          cluster configuration
   * @param vertexHandler   vertex storage handler
   * @param graphHandler    graph graph storage handler
   * @param vertexTableName vertex table name
   * @param graphTableName  graph graph table name
   * @return a graph store instance or {@code null in the case of errors}
   */
  public static GraphStore createOrOpenGraphStore(final Configuration config,
    final VertexHandler vertexHandler, final GraphHandler graphHandler,
    final String vertexTableName, final String graphTableName) {
    try {
      createTablesIfNotExists(config, vertexHandler, graphHandler,
        vertexTableName, graphTableName);

      HTable verticesTable = new HTable(config, vertexTableName);
      HTable graphsTable = new HTable(config, graphTableName);

      return new HBaseGraphStore(graphsTable, verticesTable, vertexHandler,
        graphHandler);
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
  public static void deleteGraphStore(final Configuration config) {
    deleteGraphStore(config, GConstants.DEFAULT_TABLE_VERTICES,
      GConstants.DEFAULT_TABLE_GRAPHS);
  }

  /**
   * Deletes the graph store based on the given table names.
   *
   * @param config          Hadoop configuration
   * @param vertexTableName vertex table name
   * @param graphTableName  graph table name
   */
  public static void deleteGraphStore(final Configuration config,
    final String vertexTableName, final String graphTableName) {
    try {
      deleteTablesIfExists(config, vertexTableName, graphTableName);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates the tables used by the graph store.
   *
   * @param config          Hadoop configuration
   * @param verticesHandler vertex storage handler
   * @param graphHandler    graph storage handler
   * @param vertexTableName vertex table name
   * @param graphTableName  graph table name
   * @throws IOException
   */
  private static void createTablesIfNotExists(final Configuration config,
    final VertexHandler verticesHandler, final GraphHandler graphHandler,
    final String vertexTableName, final String graphTableName) throws
    IOException {
    HTableDescriptor verticesTableDescriptor =
      new HTableDescriptor(TableName.valueOf(vertexTableName));
    HTableDescriptor graphsTableDescriptor =
      new HTableDescriptor(TableName.valueOf(graphTableName));

    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(verticesTableDescriptor.getName())) {
      verticesHandler.createTable(admin, verticesTableDescriptor);
    }
    if (!admin.tableExists(graphsTableDescriptor.getName())) {
      graphHandler.createTable(admin, graphsTableDescriptor);
    }

    admin.close();
  }

  /**
   * Deletes the tables defined by the config.
   *
   * @param config          Hadoop configuration
   * @param vertexTableName vertex table name
   * @param graphTableName  graph table name
   * @throws IOException
   */
  private static void deleteTablesIfExists(final Configuration config,
    final String vertexTableName, final String graphTableName) throws
    IOException {
    HTableDescriptor verticesTableDescriptor =
      new HTableDescriptor(TableName.valueOf(vertexTableName));
    HTableDescriptor graphsTableDescriptor =
      new HTableDescriptor(TableName.valueOf(graphTableName));

    HBaseAdmin admin = new HBaseAdmin(config);

    if (admin.tableExists(verticesTableDescriptor.getName())) {
      deleteTable(admin, verticesTableDescriptor);
    }

    if (admin.tableExists(graphsTableDescriptor.getName())) {
      deleteTable(admin, graphsTableDescriptor);
    }

    admin.close();
  }

  /**
   * Deletes a single EPG table.
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
