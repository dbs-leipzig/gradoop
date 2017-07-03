/**
 * Copyright © 2014 Gradoop (University of Leipzig - Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
