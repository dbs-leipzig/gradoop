/*
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
package org.gradoop.storage;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;

import java.util.regex.Pattern;

/**
 * Used for tests that need a HBase cluster to run.
 */
public class GradoopHBaseTestBase {

  public static final String LABEL_FORUM = "Forum";
  public static final String LABEL_TAG = "Tag";
  public static final String LABEL_HAS_MODERATOR = "hasModerator";
  public static final String LABEL_HAS_MEMBER = "hasMember";

  public static final Pattern PATTERN_GRAPH = Pattern.compile("^Com.*");
  public static final Pattern PATTERN_VERTEX = Pattern.compile("^(Per|Ta).*");
  public static final Pattern PATTERN_EDGE = Pattern.compile("^has.*");

  public static final String PROP_AGE = "age";
  public static final String PROP_SINCE = "since";
  public static final String PROP_CITY = "city";
  public static final String PROP_VERTEX_COUNT = "vertexCount";

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
   * @param env the execution environment
   * @return empty HBase graph store
   */
  public static HBaseEPGMStore createEmptyEPGMStore(ExecutionEnvironment env) {
    Configuration config = utility.getConfiguration();

    HBaseEPGMStoreFactory.deleteEPGMStore(config);
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(config,
      GradoopHBaseConfig.getDefaultConfig(env));
  }

  /**
   * Initializes and returns an empty graph store with a prefix at each table name.
   *
   * @param env the execution environment
   * @param prefix the table prefix
   * @return empty HBase graph store
   */
  public static HBaseEPGMStore createEmptyEPGMStore(ExecutionEnvironment env, String prefix) {
    Configuration config = utility.getConfiguration();

    HBaseEPGMStoreFactory.deleteEPGMStore(config, prefix);
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(
      config,
      GradoopHBaseConfig.getDefaultConfig(env),
      prefix
    );
  }

  /**
   * Open existing EPGMStore for test purposes. If the store does not exist, a
   * new one will be initialized and returned.
   *
   * @param env the execution environment
   * @return EPGMStore with vertices and edges
   */
  public static HBaseEPGMStore openEPGMStore(ExecutionEnvironment env) {
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(
      utility.getConfiguration(),
      GradoopHBaseConfig.getDefaultConfig(env)
    );
  }

  /**
   * Open existing EPGMStore for test purposes. If the store does not exist, a
   * new one will be initialized and returned.
   *
   * @param env the execution environment
   * @param prefix the table prefix
   * @return EPGMStore with vertices and edges
   */
  public static HBaseEPGMStore openEPGMStore(ExecutionEnvironment env, String prefix) {
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(
      utility.getConfiguration(),
      GradoopHBaseConfig.getDefaultConfig(env),
      prefix
    );
  }
}
