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

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
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
    testFullyConnectedGraph();
    testNonConnectedGraph();
    testSpecific();
  }

  /**
   * Test for a fully connected graph
   */
  private void testFullyConnectedGraph() throws Exception {

    LogicalGraph graph = loader.getLogicalGraphByVariable("clique");
    LogicalGraph result = graph.callForGraph(getCCAlgorithm());

    validateGraphProperties(result);

    List<Vertex> vertices = result.getVertices().collect();
    GraphHead head = result.getGraphHead().collect().get(0);

    for (Vertex v : vertices) {
      assertEquals("Wrong local value for clique-vertex '" + v.getId().toString() +
          "', should be 1", 1d,
        v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
    }
    assertEquals("Wrong average value for fully connected graph, should be 1", 1d,
      head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_AVERAGE).getDouble(), 0.0);
    assertEquals("Wrong global value for fully connected graph, should be 1", 1d,
      head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL).getDouble(), 0.0);
  }

  /**
   * Test for a graph with no connections
   */
  private void testNonConnectedGraph() throws Exception {

    LogicalGraph graph = loader.getLogicalGraphByVariable("nonConnected");
    LogicalGraph result = graph.callForGraph(getCCAlgorithm());

    validateGraphProperties(result);

    List<Vertex> vertices = result.getVertices().collect();
    GraphHead head = result.getGraphHead().collect().get(0);

    for (Vertex v : vertices) {
      assertEquals("Wrong local value for not connected vertex: " + v.getId().toString() +
        ", should be 0",0d,
        v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
    }
    double average = head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_AVERAGE).getDouble();
    assertTrue("Wrong average value for not connected graph, should be 0 or NaN",
      Double.isNaN(average));
    double global = head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL).getDouble();
    assertTrue("Wrong global value for not connected graph, should be 0 or NaN",
      Double.isNaN(global));
  }

  /**
   * Checks if clustering coefficient properties are written to vertices and graph head
   *
   * @param graph Graph with properties
   */
  void validateGraphProperties(LogicalGraph graph) throws Exception {

    List<Vertex> vertices = graph.getVertices().collect();
    GraphHead head = graph.getGraphHead().collect().get(0);

    for (Vertex v : vertices) {
      assertTrue("No local value stored in vertex: " + v.getId().toString(),
        v.hasProperty(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL));
    }

    assertTrue("No average value stored in graph head",
      head.hasProperty(ClusteringCoefficientBase.PROPERTY_KEY_AVERAGE));
    assertTrue("No global value stored in graph head",
      head.hasProperty(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL));
  }

  /**
   * Specific test for derived test-classes
   */
  public abstract void testSpecific() throws Exception;
}
