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

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test-class for {@link GellyLocalClusteringCoefficientDirected}
 */
public class GellyLocalClusteringCoefficientDirectedTest
  extends GellyClusteringCoefficientTestBase {

  @Override
  public ClusteringCoefficientBase getCCAlgorithm() {
    return new GellyLocalClusteringCoefficientDirected();
  }

  @Override
  public void testFullyConnectedGraph() throws Exception {
    validateGraphProperties(fullGraph);
    List<Vertex> vertices = fullGraph.getVertices().collect();
    for (Vertex v : vertices) {
      assertEquals(
        "Wrong local value for clique-vertex '" + v.getId().toString() + "', should be 1",
        1d,
        v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
    }
  }

  @Override
  public void testNonConnectedGraph() throws Exception {
    validateGraphProperties(nonConnectedGraph);
    List<Vertex> vertices = nonConnectedGraph.getVertices().collect();
    for (Vertex v : vertices) {
      assertEquals(
        "Wrong local value for not connected vertex: " + v.getId().toString() + ", should be 0",
        0d,
        v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
    }
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
      "(v0)-[e1]->(v2)" +
      "(v0)-[e2]->(v3)" +
      "(v1)-[e3]->(v2)" +
      "]";

    LogicalGraph graph = getLoaderFromString(graphString)
      .getLogicalGraphByVariable("halfConnected");
    LogicalGraph result = graph.callForGraph(getCCAlgorithm());

    validateGraphProperties(result);

    List<Vertex> vertices = result.getVertices().collect();
    for (Vertex v : vertices) {
      if (v.getPropertyValue("id").getInt() == 0) {
        assertEquals("vertex with id 0 has wrong local value, should be 0.1666", 1d / 6d,
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.00001);
      }
      if (v.getPropertyValue("id").getInt() == 1) {
        assertEquals("vertex with id 1 has wrong local value, should be 0.5", 1d / 2d,
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.00001);
      }
      if (v.getPropertyValue("id").getInt() == 2) {
        assertEquals("vertex with id 2 has wrong local value, should be 0.5", 1d / 2d,
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.00001);
      }
      if (v.getPropertyValue("id").getInt() == 3) {
        assertEquals("vertex with id 3 has wrong local value, should be 0", 0.0,
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
      }
    }
  }

  @Override
  public void validateGraphProperties(LogicalGraph graph) throws Exception {
    List<Vertex> vertices = graph.getVertices().collect();
    for (Vertex v : vertices) {
      assertTrue("No local value stored in vertex: " + v.getId().toString(),
        v.hasProperty(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL));
    }
  }
}
