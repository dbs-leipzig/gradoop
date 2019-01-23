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

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test-class for {@link GellyGlobalClusteringCoefficientUndirected}
 */
public class GellyGlobalClusteringCoefficientUndirectedTest
  extends GellyClusteringCoefficientTestBase {

  @Override
  public ClusteringCoefficientBase getCCAlgorithm() {
    return new GellyGlobalClusteringCoefficientUndirected();
  }

  @Override
  public void testFullyConnectedGraph() throws Exception {
    validateGraphProperties(fullGraph);
    GraphHead head = fullGraph.getGraphHead().collect().get(0);
    assertEquals("Wrong global value for fully connected graph, should be 1", 1d,
      head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL).getDouble(), 0.0);
  }

  @Override
  public void testNonConnectedGraph() throws Exception {
    validateGraphProperties(nonConnectedGraph);
    GraphHead head = nonConnectedGraph.getGraphHead().collect().get(0);
    double global = head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL)
      .getDouble();
    assertTrue("Wrong global value for not connected graph, should be 0 or NaN",
      Double.isNaN(global));
  }

  @Override
  public void testSpecific() throws Exception {
    String graphString = "halfConnected[" +
      "/* half connected graph */" +
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
      "(v3 {id:3, value:\"D\"})" +
      "(v0)-[e0]->(v1)" +
      "(v1)-[e1]->(v0)" +
      "(v0)-[e2]->(v2)" +
      "(v2)-[e3]->(v0)" +
      "(v0)-[e4]->(v3)" +
      "(v3)-[e5]->(v0)" +
      "(v1)-[e6]->(v2)" +
      "(v2)-[e7]->(v1)" +
      "]";

    LogicalGraph graph = getLoaderFromString(graphString)
      .getLogicalGraphByVariable("halfConnected");
    LogicalGraph result = graph.callForGraph(getCCAlgorithm());

    validateGraphProperties(result);

    GraphHead head = result.getGraphHead().collect().get(0);

    double global = head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL)
      .getDouble();
    assertEquals("graph has wrong global value, should be 0.6",
      3d / 5d, global, 0.0);
  }

  @Override
  public void validateGraphProperties(LogicalGraph graph) throws Exception {
    GraphHead head = graph.getGraphHead().collect().get(0);
    assertTrue("No global value stored in graph head",
      head.hasProperty(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL));
  }
}
