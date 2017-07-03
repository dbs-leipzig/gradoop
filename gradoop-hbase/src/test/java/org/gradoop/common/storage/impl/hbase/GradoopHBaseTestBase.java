
package org.gradoop.common.storage.impl.hbase;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.gradoop.common.config.GradoopHBaseConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStoreFactory;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Used for tests that need a HBase cluster to run.
 */
public class GradoopHBaseTestBase extends GradoopFlinkTestBase{

  //----------------------------------------------------------------------------
  // Cluster related
  //----------------------------------------------------------------------------

  /**
   * Handles the test cluster which is started for during unit testing.
   */
  protected static HBaseTestingUtility utility;

  /**
   * Starts the mini cluster for all tests.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUpHBase() throws Exception {
    if (utility == null) {
      utility = new HBaseTestingUtility(HBaseConfiguration.create());
      utility.startMiniCluster().waitForActiveAndReadyMaster();
    }
  }

  /**
   * Stops the test cluster after the test.
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDownHBase() throws Exception {
    if (utility != null) {
      utility.shutdownMiniCluster();
      utility = null;
    }
  }

  //----------------------------------------------------------------------------
  // Store handling methods
  //----------------------------------------------------------------------------

  /**
   * Initializes and returns an empty graph store.
   *
   * @return empty HBase graph store
   */
  public static HBaseEPGMStore<GraphHead, Vertex, Edge> createEmptyEPGMStore(ExecutionEnvironment env) {
    Configuration config = utility.getConfiguration();

    HBaseEPGMStoreFactory.deleteEPGMStore(config);
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(config,
      GradoopHBaseConfig.getDefaultConfig(env));
  }

  /**
   * Open existing EPGMStore for test purposes. If the store does not exist, a
   * new one will be initialized and returned.
   *
   * @return EPGMStore with vertices and edges
   */
  public static HBaseEPGMStore<GraphHead, Vertex, Edge> openEPGMStore(ExecutionEnvironment env) {
    Configuration config = utility.getConfiguration();

    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(config,
      GradoopHBaseConfig.getDefaultConfig(env));
  }
}
