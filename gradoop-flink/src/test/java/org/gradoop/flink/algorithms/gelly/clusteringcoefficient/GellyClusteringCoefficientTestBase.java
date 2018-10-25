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
package org.gradoop.flink.algorithms.gelly.clusteringcoefficient;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Base test-class for clustering coefficient algorithm wrapper
 */
public abstract class GellyClusteringCoefficientTestBase extends GradoopFlinkTestBase {

  /**
   * GraphLoader used for test-cases
   */
  protected FlinkAsciiGraphLoader loader;

  /**
   * Constructor
   */
  public GellyClusteringCoefficientTestBase() {

    String graphString =
      "/* vertices */" +
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
      "clique[" +
      "/* fully connected clique */" +
      "(v0)-[e0]->(v1)" +
      "(v1)-[e1]->(v0)" +
      "(v0)-[e2]->(v2)" +
      "(v2)-[e3]->(v0)" +
      "(v1)-[e4]->(v2)" +
      "(v2)-[e5]->(v1)" +
      "]" +
      "nonConnected[" +
      "/* not connected vertices */" +
      "]";

    this.loader = getLoaderFromString(graphString);
  }

  /**
   * Gets the clustering coefficient algorithm wrapper used for testing.
   *
   * @return The clustering coefficient algorithm wrapper
   */
  public abstract ClusteringCoefficientBase getCCAlgorithm();

  /**
   * Runs the specified tests
   */
  @Test
  public void runTests() throws Exception {
    LogicalGraph fullGraph = loader.getLogicalGraphByVariable("clique");
    fullGraph = fullGraph.callForGraph(getCCAlgorithm());
    validateGraphProperties(fullGraph);
    testFullyConnectedGraph(fullGraph);

    LogicalGraph nonConnectedGraph = loader.getLogicalGraphByVariable("nonConnected");
    nonConnectedGraph = nonConnectedGraph.callForGraph(getCCAlgorithm());
    validateGraphProperties(nonConnectedGraph);
    testNonConnectedGraph(nonConnectedGraph);

    testSpecific();
  }

  /**
   * Test for a fully connected graph
   */
  public abstract void testFullyConnectedGraph(LogicalGraph graph) throws Exception;

  /**
   * Test for a graph with no connections
   */
  public abstract void testNonConnectedGraph(LogicalGraph graph) throws Exception;

  /**
   * Test for a specific graph regarding directed vs. undirected
   */
  public abstract void testSpecific() throws Exception;

  /**
   * Checks if clustering coefficient properties are written
   *
   * @param graph Graph with properties
   */
  public abstract void validateGraphProperties(LogicalGraph graph) throws Exception;
}
