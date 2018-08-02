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
package org.gradoop.storage.impl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;

import java.io.IOException;
import java.util.Collection;
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
  public static final Pattern PATTERN_GRAPH_PROP = Pattern.compile(".*doop$");
  public static final Pattern PATTERN_VERTEX_PROP = Pattern.compile(".*ve$");
  public static final Pattern PATTERN_EDGE_PROP = Pattern.compile("^start..$");

  public static final String PROP_AGE = "age";
  public static final String PROP_CITY = "city";
  public static final String PROP_INTEREST = "interest";
  public static final String PROP_NAME = "name";
  public static final String PROP_SINCE = "since";
  public static final String PROP_STATUS = "status";
  public static final String PROP_VERTEX_COUNT = "vertexCount";

  private static Collection<GraphHead> socialGraphHeads;
  private static Collection<Vertex> socialVertices;
  private static Collection<Edge> socialEdges;

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
   * @throws Exception if setting up HBase test cluster fails
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
   * @throws Exception if closing HBase test cluster fails
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
  public static HBaseEPGMStore createEmptyEPGMStore() {
    Configuration config = utility.getConfiguration();

    HBaseEPGMStoreFactory.deleteEPGMStore(config);
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(config,
      GradoopHBaseConfig.getDefaultConfig());
  }

  /**
   * Initializes and returns an empty graph store with a prefix at each table name.
   *
   * @param prefix the table prefix
   * @return empty HBase graph store
   */
  public static HBaseEPGMStore createEmptyEPGMStore(String prefix) {
    Configuration config = utility.getConfiguration();

    HBaseEPGMStoreFactory.deleteEPGMStore(config, prefix);
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(
      config,
      GradoopHBaseConfig.getDefaultConfig(),
      prefix
    );
  }

  /**
   * Open existing EPGMStore for test purposes. If the store does not exist, a
   * new one will be initialized and returned.
   *
   * @return EPGMStore with vertices and edges
   */
  public static HBaseEPGMStore openEPGMStore() {
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(
      utility.getConfiguration(),
      GradoopHBaseConfig.getDefaultConfig()
    );
  }

  /**
   * Open existing EPGMStore for test purposes. If the store does not exist, a
   * new one will be initialized and returned.
   *
   * @param prefix the table prefix
   * @return EPGMStore with vertices and edges
   */
  public static HBaseEPGMStore openEPGMStore(String prefix) {
    return HBaseEPGMStoreFactory.createOrOpenEPGMStore(
      utility.getConfiguration(),
      GradoopHBaseConfig.getDefaultConfig(),
      prefix
    );
  }


  //----------------------------------------------------------------------------
  // Data generation
  //----------------------------------------------------------------------------

  /**
   * Creates a collection of graph heads according to the social network test graph in
   * gradoop/dev-support/social-network.pdf.
   *
   * @return collection of graph heads
   * @throws IOException if fetching graph elements failed
   */
  public static Collection<GraphHead> getSocialGraphHeads() throws IOException {
    if (socialGraphHeads == null) {
      socialGraphHeads = GradoopTestUtils.getSocialNetworkLoader().getGraphHeads();
    }
    return socialGraphHeads;
  }

  /**
   * Creates a collection of vertices according to the social
   * network test graph in gradoop/dev-support/social-network.pdf.
   *
   * @return collection of vertices
   * @throws IOException if fetching graph elements failed
   */
  public static Collection<Vertex> getSocialVertices() throws IOException {
    if (socialVertices == null) {
      socialVertices = GradoopTestUtils.getSocialNetworkLoader().getVertices();
    }
    return socialVertices;
  }

  /**
   * Creates a collection of edges according to the social
   * network test graph in gradoop/dev-support/social-network.pdf.
   *
   * @return collection of edges
   * @throws IOException if fetching graph elements failed
   */
  public static Collection<Edge> getSocialEdges() throws IOException {
    if (socialEdges == null) {
      socialEdges = GradoopTestUtils.getSocialNetworkLoader().getEdges();
    }
    return socialEdges;
  }

  /**
   * Writes the example social graph to the HBase store and flushes it.
   *
   * @param epgmStore the store instance to write to
   * @throws IOException if writing to store fails
   */
  public static void writeSocialGraphToStore(HBaseEPGMStore epgmStore) throws IOException {
    // write social graph to HBase
    for (GraphHead g : getSocialGraphHeads()) {
      epgmStore.writeGraphHead(g);
    }
    for (Vertex v : getSocialVertices()) {
      epgmStore.writeVertex(v);
    }
    for (Edge e : getSocialEdges()) {
      epgmStore.writeEdge(e);
    }
    epgmStore.flush();
  }

}
