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

import org.apache.commons.math3.util.Precision;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test-class for {@link GellyClusteringCoefficientUndirected}
 */
public class GellyClusteringCoefficientUndirectedTest extends GellyClusteringCoefficientTestBase {

  /**
   * Creates an instance of GellyClusteringCoefficientUndirectedTest.
   * Calls constructor of super class.
   */
  public GellyClusteringCoefficientUndirectedTest() {
    super();
  }

  @Override
  public ClusteringCoefficientBase getCCAlgorithm() {
    return new GellyClusteringCoefficientUndirected();
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

    LogicalGraph graph = getLoaderFromString(graphString).getLogicalGraphByVariable("halfConnected");
    LogicalGraph result = graph.callForGraph(getCCAlgorithm());

    validateGraphProperties(result);

    List<Vertex> vertices = result.getVertices().collect();
    GraphHead head = result.getGraphHead().collect().get(0);

    for (Vertex v : vertices) {
      if (v.getPropertyValue("id").getInt() == 0) {
        assertEquals("vertex with id 0 has wrong local value, should be 0.3333", (1d/3d),
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
      }
      if (v.getPropertyValue("id").getInt() == 1) {
        assertEquals("vertex with id 1 has wrong local value, should be 1", 1d,
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
      }
      if (v.getPropertyValue("id").getInt() == 2) {
        assertEquals("vertex with id 2 has wrong local value, should be 1", 1d,
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
      }
      if (v.getPropertyValue("id").getInt() == 3) {
        assertEquals("vertex with id 3 has wrong local value, should be 0 or NaN",
          Double.NaN,
          v.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL).getDouble(), 0.0);
      }
    }

    double average = head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_AVERAGE)
      .getDouble();
    assertEquals("graph has wrong average value, should be 0.583",
      Precision.round((1d + 1d + (1d/3d) + 0d) / 4d, 3),
      Precision.round(average, 3), 0.0);

    double global = head.getPropertyValue(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL)
      .getDouble();
    assertEquals("graph has wrong global value, should be 0.6",
      Precision.round(3d / 5d, 3),
      Precision.round(global, 3), 0.0);
  }
}
