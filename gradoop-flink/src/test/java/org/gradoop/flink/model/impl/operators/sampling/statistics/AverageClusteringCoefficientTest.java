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
package org.gradoop.flink.model.impl.operators.sampling.statistics;

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.AverageClusteringCoefficient;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test-class for {@link AverageClusteringCoefficient}
 */
public class AverageClusteringCoefficientTest extends GradoopFlinkTestBase {

  /**
   * GraphLoader used for test-cases
   */
  private FlinkAsciiGraphLoader loader;

  /**
   * Initialize the graphs for testing
   */
  @Before
  public void initGraphs() {
    String graphString = "/* vertices */" +
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
      "(v3 {id:3, value:\"D\"})" +
      "(v4 {id:4, value:\"E\"})" +
      "(v5 {id:5, value:\"F\"})" +
      "]" +
      "halfConnected[" +
      "/* half connected graph */" +
      "(v6 {id:6, value:\"G\"})" +
      "(v0)-[e6]->(v1)" +
      "(v0)-[e7]->(v2)" +
      "(v0)-[e8]->(v6)" +
      "(v1)-[e9]->(v2)" +
      "]";

    this.loader = getLoaderFromString(graphString);
  }

  /**
   * Test for a fully connected graph
   */
  @Test
  public void testFullyConnectedGraph() throws Exception {

    LogicalGraph graph = loader.getLogicalGraphByVariable("clique");
    LogicalGraph result = graph.callForGraph(new AverageClusteringCoefficient());
    validateGraphProperties(result);

    GraphHead head = result.getGraphHead().collect().get(0);

    assertEquals("Wrong average value for fully connected graph, should be 1",
      1d, head.getPropertyValue(AverageClusteringCoefficient.PROPERTY_KEY_AVERAGE)
        .getDouble(), 0.0);
  }

  /**
   * Test for a graph with no connections
   */
  @Test
  public void testNonConnectedGraph() throws Exception {

    LogicalGraph graph = loader.getLogicalGraphByVariable("nonConnected");
    LogicalGraph result = graph.callForGraph(new AverageClusteringCoefficient());
    validateGraphProperties(result);

    GraphHead head = result.getGraphHead().collect().get(0);

    double average = head.getPropertyValue(AverageClusteringCoefficient.PROPERTY_KEY_AVERAGE)
      .getDouble();
    assertEquals("Wrong average value for not connected graph, should be 0.0",
      0.0, average, 0.0);
  }

  /**
   * Test for a half connected graph
   */
  @Test
  public void testHalfConnectedGraph() throws Exception {

    LogicalGraph graph = loader.getLogicalGraphByVariable("halfConnected");
    LogicalGraph result = graph.callForGraph(new AverageClusteringCoefficient());
    validateGraphProperties(result);

    GraphHead head = result.getGraphHead().collect().get(0);

    double average = head.getPropertyValue(AverageClusteringCoefficient.PROPERTY_KEY_AVERAGE)
      .getDouble();
    assertEquals("graph has wrong average value, should be 0.2916",
      ((1d / 6d) + (1d / 2d) + (1d / 2d) + 0d) / 4d, average, 0.00001);
  }

  /**
   * Checks if clustering coefficient properties are written to graph head
   *
   * @param graph Graph with properties
   */
  private void validateGraphProperties(LogicalGraph graph) throws Exception {
    GraphHead head = graph.getGraphHead().collect().get(0);
    assertTrue("No average value stored in graph head",
      head.hasProperty(AverageClusteringCoefficient.PROPERTY_KEY_AVERAGE));
  }
}
