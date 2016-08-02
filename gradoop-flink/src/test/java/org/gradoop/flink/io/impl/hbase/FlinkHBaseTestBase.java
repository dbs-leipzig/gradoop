package org.gradoop.flink.io.impl.hbase;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.common.storage.impl.hbase.GradoopHBaseTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Used for tests that require a HBase and Flink cluster up and running.
 */
public class FlinkHBaseTestBase extends GradoopFlinkTestBase {

  /**
   * Start Flink and HBase cluster.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    GradoopFlinkTestBase.setup();
    GradoopHBaseTestBase.setUp();
  }

  /**
   * Stop Flink and HBase cluster.
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDown() throws Exception {
    GradoopFlinkTestBase.tearDown();
    GradoopHBaseTestBase.tearDown();
  }
}
