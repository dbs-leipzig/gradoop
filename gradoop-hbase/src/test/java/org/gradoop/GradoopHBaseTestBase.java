/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop;

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

/**
 * Used for tests that need a HBase cluster to run.
 */
public class GradoopHBaseTestBase {

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
