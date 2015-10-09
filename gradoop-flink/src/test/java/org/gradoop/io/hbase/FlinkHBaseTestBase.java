package org.gradoop.io.hbase;

import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.gradoop.HBaseTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Used for tests that require a HBase and Flink cluster up and running.
 */
public class FlinkHBaseTestBase extends MultipleProgramsTestBase {

  public FlinkHBaseTestBase(TestExecutionMode mode) {
    super(mode);
  }

  /**
   * Start Flink and HBase cluster.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    MultipleProgramsTestBase.setup();
    HBaseTestBase.setUp();
  }

  /**
   * Stop Flink and HBase cluster.
   *
   * @throws Exception
   */
  @AfterClass
  public static void teardown() throws Exception {
    MultipleProgramsTestBase.teardown();
    HBaseTestBase.tearDown();
  }
}
