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
package org.gradoop.flink.algorithms.gelly.trianglecounting;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link GellyTriangleCounting}
 */
public class GellyTriangleCountingTest extends GradoopFlinkTestBase {

  /**
   * Test triangle counting for cases:
   * <pre>
   *   - open triplet
   *   - 2 triangles in a directed graph
   *   - 2 triangles in an undirected graph
   *   - 8 triangles in directed social graph from {@link GradoopTestUtils#SOCIAL_NETWORK_GDL_FILE}
   * </pre>
   */
  @Test
  public void testTriangleCounting() throws Exception {

    String graphString =
      "/* all vertices */" +
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
      "(v3 {id:3, value:\"D\"})" +
      "(v4 {id:4, value:\"E\"})" +
      "(v5 {id:5, value:\"F\"})" +
      "triplet[" +
      "/* 1 open triplet */" +
      "(v0)-[e0]->(v1)" +
      "(v0)-[e1]->(v2)" +
      "]" +
      "trianglesDirected[" +
      "/* 2 triangles in directed graph */" +
      "(v0)-[e2]->(v1)" +
      "(v0)-[e3]->(v2)" +
      "(v1)-[e4]->(v2)" +
      "(v0)-[e5]->(v3)" +
      "(v3)-[e6]->(v4)" +
      "(v4)-[e7]->(v5)" +
      "(v5)-[e8]->(v3)" +
      "]" +
      "trianglesUndirected[" +
      "/* 2 triangles in undirected graph */" +
      "(v0)-[e9]->(v1)" +
      "(v1)-[e10]->(v0)" +
      "(v0)-[e11]->(v2)" +
      "(v2)-[e12]->(v0)" +
      "(v1)-[e13]->(v2)" +
      "(v2)-[e14]->(v1)" +
      "(v0)-[e15]->(v3)" +
      "(v3)-[e16]->(v0)" +
      "(v3)-[e17]->(v4)" +
      "(v4)-[e18]->(v3)" +
      "(v4)-[e19]->(v5)" +
      "(v5)-[e20]->(v4)" +
      "(v5)-[e21]->(v3)" +
      "(v3)-[e22]->(v5)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(graphString);

    LogicalGraph tripletGraph = loader.getLogicalGraphByVariable("triplet");
    LogicalGraph trianglesDirectedGraph = loader.getLogicalGraphByVariable("trianglesDirected");
    LogicalGraph trianglesUndirectedGraph = loader.getLogicalGraphByVariable("trianglesUndirected");

    tripletGraph = tripletGraph.callForGraph(new GellyTriangleCounting());
    trianglesDirectedGraph = trianglesDirectedGraph.callForGraph(new GellyTriangleCounting());
    trianglesUndirectedGraph = trianglesUndirectedGraph.callForGraph(new GellyTriangleCounting());

    assertEquals("Wrong number of triangles for triplet, should be 0L",0L,
      tripletGraph.getGraphHead().collect().get(0).getPropertyValue(
        GellyTriangleCounting.PROPERTY_KEY_TRIANGLES).getLong());

    assertEquals("Wrong number of triangles for directed graph, should be 2L",2L,
      trianglesDirectedGraph.getGraphHead().collect().get(0).getPropertyValue(
        GellyTriangleCounting.PROPERTY_KEY_TRIANGLES).getLong());

    assertEquals("Wrong number of triangles for undirected graph, should be 2L",2L,
      trianglesUndirectedGraph.getGraphHead().collect().get(0).getPropertyValue(
        GellyTriangleCounting.PROPERTY_KEY_TRIANGLES).getLong());

    LogicalGraph socialGraph = getSocialNetworkLoader().getLogicalGraph();
    socialGraph = socialGraph.callForGraph(new GellyTriangleCounting());

    assertEquals("Wrong number of triangles for social graph, should be 8L",8L,
      socialGraph.getGraphHead().collect().get(0).getPropertyValue(
        GellyTriangleCounting.PROPERTY_KEY_TRIANGLES).getLong());
  }
}
