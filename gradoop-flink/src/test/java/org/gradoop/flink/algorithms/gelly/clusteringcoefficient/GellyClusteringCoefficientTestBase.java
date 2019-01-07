/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.gelly.clusteringcoefficient;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

/**
 * Base test-class for clustering coefficient algorithm wrapper
 */
public abstract class GellyClusteringCoefficientTestBase extends GradoopFlinkTestBase {

  /**
   * A fully connected graph for testing
   */
  LogicalGraph fullGraph;

  /**
   * A not connected graph for testing
   */
  LogicalGraph nonConnectedGraph;

  /**
   * Initialize the graphs for testing
   */
  @Before
  public void initGraphs() {
    String graphString = "clique[" +
      "/* fully connected clique */" +
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
      "(v0)-[e0]->(v1)" +
      "(v1)-[e1]->(v0)" +
      "(v0)-[e2]->(v2)" +
      "(v2)-[e3]->(v0)" +
      "(v1)-[e4]->(v2)" +
      "(v2)-[e5]->(v1)" +
      "]" +
      "nonConnected[" +
      "/* not connected vertices */" +
      "(v3 {id:3, value:\"D\"})" +
      "(v4 {id:4, value:\"E\"})" +
      "(v5 {id:5, value:\"F\"})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(graphString);
    fullGraph = loader.getLogicalGraphByVariable("clique");
    fullGraph = fullGraph.callForGraph(getCCAlgorithm());

    nonConnectedGraph = loader.getLogicalGraphByVariable("nonConnected");
    nonConnectedGraph = nonConnectedGraph.callForGraph(getCCAlgorithm());
  }

  /**
   * Gets the clustering coefficient algorithm wrapper used for testing.
   *
   * @return The clustering coefficient algorithm wrapper
   */
  public abstract ClusteringCoefficientBase getCCAlgorithm();

  /**
   * Checks if clustering coefficient properties are written
   */
  public abstract void validateGraphProperties(LogicalGraph graph) throws Exception;

  /**
   * Test for a fully connected graph
   */
  @Test
  public abstract void testFullyConnectedGraph() throws Exception;

  /**
   * Test for a graph with no connections
   */
  @Test
  public abstract void testNonConnectedGraph() throws Exception;

  /**
   * Test for a specific graph regarding directed vs. undirected
   */
  @Test
  public abstract void testSpecific() throws Exception;
}
