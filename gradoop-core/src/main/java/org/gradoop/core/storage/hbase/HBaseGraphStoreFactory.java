package org.gradoop.core.storage.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;
import org.gradoop.core.storage.GraphStore;

import java.io.IOException;

/**
 * Created by martin on 05.11.14.
 */
public class HBaseGraphStoreFactory {

  private static final Logger LOG =
    Logger.getLogger(HBaseGraphStoreFactory.class);

  private HBaseGraphStoreFactory() {
  }

  public static GraphStore createGraphStore(Configuration config,
                                            VertexHandler verticesHandler,
                                            GraphHandler graphsHandler) {
    HTable graphsTable = null;
    HTable verticesTable = null;

    try {
      createTablesIfNotExists(config, verticesHandler);

      graphsTable = new HTable(config, HBaseGraphStore.TABLE_GRAPHS);
      verticesTable = new HTable(config, HBaseGraphStore.TABLE_VERTICES);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new HBaseGraphStore(graphsTable, verticesTable, verticesHandler,
      graphsHandler);
  }

  public static void deleteGraphStore(Configuration config) {
    try {
      deleteTablesIfExists(config);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void createTablesIfNotExists(Configuration config,
                                              VertexHandler verticesHandler)
    throws IOException {
    HTableDescriptor verticesTableDescriptor =
      new HTableDescriptor(TableName.valueOf(HBaseGraphStore.TABLE_VERTICES));
    HTableDescriptor graphsTableDescriptor =
      new HTableDescriptor(TableName.valueOf(HBaseGraphStore.TABLE_GRAPHS));

    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(verticesTableDescriptor.getName())) {
      verticesHandler.createVerticesTable(admin, verticesTableDescriptor);
      //      createVerticesTable(admin, verticesTableDescriptor);
    }
    if (!admin.tableExists(graphsTableDescriptor.getName())) {
      createGraphsTable(admin, graphsTableDescriptor);
    }

    admin.close();
  }

  //  private static void createVerticesTable(HBaseAdmin admin,
  // HTableDescriptor tableDescriptor)
  //      throws IOException {
  //    LOG.info("creating table " + tableDescriptor.getNameAsString());
  //    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore
  // .CF_LABELS));
  //    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore
  // .CF_PROPERTIES));
  //    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore
  // .CF_OUT_EDGES));
  //    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore
  // .CF_IN_EDGES));
  //    admin.createTable(tableDescriptor);
  //  }

  private static void createGraphsTable(HBaseAdmin admin,
                                        HTableDescriptor tableDescriptor)
    throws IOException {
    LOG.info("creating table " + tableDescriptor.getNameAsString());
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore.CF_LABELS));
    tableDescriptor
      .addFamily(new HColumnDescriptor(HBaseGraphStore.CF_PROPERTIES));
    tableDescriptor
      .addFamily(new HColumnDescriptor(HBaseGraphStore.CF_VERTICES));
    admin.createTable(tableDescriptor);
  }

  private static void deleteTablesIfExists(Configuration config)
    throws IOException {
    HTableDescriptor verticesTableDescriptor =
      new HTableDescriptor(TableName.valueOf(HBaseGraphStore.TABLE_VERTICES));
    HTableDescriptor graphsTableDescriptor =
      new HTableDescriptor(TableName.valueOf(HBaseGraphStore.TABLE_GRAPHS));

    HBaseAdmin admin = new HBaseAdmin(config);

    if (admin.tableExists(verticesTableDescriptor.getName())) {
      deleteTable(admin, verticesTableDescriptor);
    }

    if (admin.tableExists(graphsTableDescriptor.getName())) {
      deleteTable(admin, graphsTableDescriptor);
    }

    admin.close();
  }

  private static void deleteTable(HBaseAdmin admin,
                                  HTableDescriptor tableDescriptor)
    throws IOException {
    LOG.info("deleting table: " + tableDescriptor.getNameAsString());
    admin.disableTable(tableDescriptor.getName());
    admin.deleteTable(tableDescriptor.getName());
  }
}
