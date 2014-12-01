package org.gradoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.HBaseGraphStoreFactory;
import org.gradoop.storage.hbase.VertexHandler;
import org.junit.BeforeClass;

/**
 * Used for test cases that need a HDFS/HBase mini cluster to run. Initializes a
 * test cluster before the first test runs and shuts it down after all tests.
 */
public abstract class HBaseClusterTest extends GradoopTest {

  protected static HBaseTestingUtility utility;

  protected GraphStore createEmptyGraphStore() {
    Configuration config = utility.getConfiguration();
    VertexHandler verticesHandler = new EPGVertexHandler();
    GraphHandler graphsHandler = new EPGGraphHandler();

    HBaseGraphStoreFactory.deleteGraphStore(config);
    return HBaseGraphStoreFactory
      .createGraphStore(config, verticesHandler, graphsHandler);
  }

  protected GraphStore openBasicGraphStore() {
    Configuration config = utility.getConfiguration();
    VertexHandler verticesHandler = new EPGVertexHandler();
    GraphHandler graphsHandler = new EPGGraphHandler();
    return HBaseGraphStoreFactory
      .createGraphStore(config, verticesHandler, graphsHandler);
  }

  /**
   * Starts the mini cluster once for all test cases implementing this class.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup()
    throws Exception {
    if (utility == null) {
      utility = new HBaseTestingUtility();
      utility.startMiniCluster().waitForActiveAndReadyMaster();
    }
  }
}
