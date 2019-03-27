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
package org.gradoop.flink.algorithms.gelly.pagerank;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * A test for {@link PageRank}, calling the operator and checking if the result was set.
 */
public class PageRankTest extends GradoopFlinkTestBase {

  /**
   * Property key to store the page rank in.
   */
  private final String propertyKey = "pageRankScore";

  /**
   * graph for testing
   */
  private LogicalGraph testGraph;

  /**
   * Initialize the graph for testing
   */
  @Before
  public void prepareTestGraph() {
    String graphString = "graph[" +
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
      "(v3 {id:3, value:\"D\"})" +
      "(v0)-[e0]->(v1)" +
      "(v1)-[e1]->(v0)" +
      "(v0)-[e2]->(v2)" +
      "(v2)-[e3]->(v0)" +
      "(v1)-[e4]->(v2)" +
      "(v2)-[e5]->(v1)" +
      "]";
    FlinkAsciiGraphLoader loader = getLoaderFromString(graphString);
    testGraph = loader.getLogicalGraphByVariable("graph");
  }

  /**
   * Check PageRank for excluded "zero-degree" vertices
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testPageRankWithoutZeroDegrees() throws Exception {
    LogicalGraph resultGraph = new PageRank(propertyKey, 0.3, 20)
      .execute(testGraph);
    checkPageRankProperty(resultGraph);
    assertEquals(resultGraph.getVertices().count(), 3L);
  }

  /**
   * Check PageRank for included "zero-degree" vertices
   *
   * @throws Exception If the execution fails.
   */
  @Test
  public void testPageRankWithZeroDegrees() throws Exception {
    LogicalGraph resultGraph = new PageRank(propertyKey, 0.3, 20, true)
      .execute(testGraph);
    checkPageRankProperty(resultGraph);
    assertEquals(resultGraph.getVertices().count(), testGraph.getVertices().count());
  }

  /**
   * Checks if the PageRank property exists and its value was initialized
   *
   * @param graph The result graph
   */
  private void checkPageRankProperty(LogicalGraph graph) throws Exception {
    List<Vertex> vertices = graph.getVertices().collect();
    for (Vertex vertex : vertices) {
      assertTrue(vertex.hasProperty(propertyKey));
      assertTrue(vertex.getPropertyValue(propertyKey).getDouble() > 0d);
    }
  }
}
