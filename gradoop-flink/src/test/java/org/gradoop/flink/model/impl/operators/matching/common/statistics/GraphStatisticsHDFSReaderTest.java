package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class GraphStatisticsHDFSReaderTest extends GraphStatisticsTest {
  private static HBaseTestingUtility utility;

  @BeforeClass
  public static void setUp() throws Exception {
    if (utility == null) {
      utility = new HBaseTestingUtility(HBaseConfiguration.create());
      utility.startMiniCluster().waitForActiveAndReadyMaster();
    }

    // copy test resources to HDFS
    Path localPath = new Path(GraphStatisticsHDFSReaderTest.class.getResource("/data/json/sna/statistics").getFile());
    Path remotePath = new Path("/");
    utility.getTestFileSystem().copyFromLocalFile(localPath, remotePath);

    // read graph statistics from HDFS
    TEST_STATISTICS = GraphStatisticsHDFSReader.read("hdfs:///statistics", utility.getConfiguration());
  }

  /**
   * Stops the test cluster after the test.
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDown() throws Exception {
    if (utility != null) {
      utility.shutdownMiniCluster();
    }
  }
}
