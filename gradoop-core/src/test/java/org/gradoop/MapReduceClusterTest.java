package org.gradoop;

import org.junit.BeforeClass;

/**
 * Tests that need a MapReduce cluster should extend this.
 */
public class MapReduceClusterTest extends HBaseClusterTest {
  /**
   * Start an additional MapReduce Cluster for all test cases extending this
   * class.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup()
    throws Exception {
    if (utility == null) {
      HBaseClusterTest.setup();
      utility.startMiniMapReduceCluster();
    }
  }
}
