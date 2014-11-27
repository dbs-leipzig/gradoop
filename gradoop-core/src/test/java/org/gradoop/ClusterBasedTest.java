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
 * Created by martin on 12.11.14.
 */
public abstract class ClusterBasedTest extends GradoopTest {

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
