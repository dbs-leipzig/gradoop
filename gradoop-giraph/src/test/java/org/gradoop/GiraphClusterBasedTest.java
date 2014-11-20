package org.gradoop;

import org.apache.giraph.BspCase;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Created by martin on 20.11.14.
 */
public abstract class GiraphClusterBasedTest extends ClusterBasedTest {

  /**
   * Reference to {@link org.apache.giraph.BspCase} for cluster setup.
   */
  private final BspCase bspCase;

  public GiraphClusterBasedTest(String testName) {
    this.bspCase = new BspCase(testName);
  }

  public final Configuration setupConfiguration(GiraphJob job)
    throws IOException {
    return this.bspCase.setupConfiguration(job);
  }

  /**
   * Start an additional MapReduce Cluster
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup()
    throws Exception {
    ClusterBasedTest.setup();
    utility.startMiniMapReduceCluster();
  }
}
