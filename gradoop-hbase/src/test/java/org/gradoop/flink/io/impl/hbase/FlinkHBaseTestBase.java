package org.gradoop.flink.io.impl.hbase;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.common.storage.impl.hbase.GradoopHBaseTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class FlinkHBaseTestBase extends GradoopFlinkTestBase {

  /**
   * Start Flink and HBase cluster.
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    GradoopFlinkTestBase.setupFlink();
    GradoopHBaseTestBase.setUpHBase();
  }

  /**
   * Stop Flink and HBase cluster.
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDown() throws Exception {
    GradoopHBaseTestBase.tearDownHBase();
    GradoopFlinkTestBase.tearDownFlink();
  }
}
