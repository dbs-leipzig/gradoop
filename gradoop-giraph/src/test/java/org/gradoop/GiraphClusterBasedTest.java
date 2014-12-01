package org.gradoop;

import org.apache.giraph.BspCase;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Tests that need to test Giraph on a mini cluster should extend this.
 */
public abstract class GiraphClusterBasedTest extends MapReduceClusterTest {

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
}
