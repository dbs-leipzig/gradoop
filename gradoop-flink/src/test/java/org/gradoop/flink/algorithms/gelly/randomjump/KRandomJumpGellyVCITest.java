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
package org.gradoop.flink.algorithms.gelly.randomjump;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingConstants;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link KRandomJumpGellyVCI}
 */
public class KRandomJumpGellyVCITest extends GradoopFlinkTestBase {
  /**
   * The social graph used for testing
   */
  private LogicalGraph socialGraph;

  /**
   * The custom graph used for testing
   */
  private LogicalGraph customGraph;

  /**
   * List for result vertices
   */
  private List<EPGMVertex> resultVertices;

  /**
   * Initialize graphs for testing
   */
  @Before
  public void initGraphs() throws Exception {
    socialGraph = getSocialNetworkLoader().getLogicalGraph();
    String graphString = "graph[" +
      "/* no edges graph */" +
      "(v0 {id:0, value:\"A\"})" +
      "(v1 {id:1, value:\"B\"})" +
      "(v2 {id:2, value:\"C\"})" +
      "]";
    customGraph = getLoaderFromString(graphString).getLogicalGraphByVariable("graph");
  }

  /**
   * Test with social graph, with 1 starting vertex and at least half of the vertices to visit.
   */
  @Test
  public void baseTest() throws Exception {
    LogicalGraph result = new KRandomJumpGellyVCI(1, 1000, 0.15,
      0.5).execute(socialGraph);

    commonValidation(socialGraph, result);

    long visitedVertices = resultVertices.stream().filter(
      vertex -> vertex.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean())
      .count();
    assertTrue("Wrong number of visited vertices, should be at least 6",
      visitedVertices >= 6L);
  }

  /**
   * Test with social graph, with 3 starting vertices and at least half of the vertices to visit.
   */
  @Test
  public void base3StartVerticesTest() throws Exception {
    LogicalGraph result = new KRandomJumpGellyVCI(3, 1000, 0.15,
      0.5).execute(socialGraph);

    commonValidation(socialGraph, result);

    long visitedVertices = resultVertices.stream().filter(
      vertex -> vertex.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean())
      .count();
    assertTrue("Wrong number of visited vertices, should be at least 6",
      visitedVertices >= 6L);
  }

  /**
   * Test with social graph, with 1 starting vertex and all of the vertices to visit.
   */
  @Test
  public void visitAllTest() throws Exception {
    LogicalGraph result = new KRandomJumpGellyVCI(1, 1000, 0.15,
      1.0).execute(socialGraph);

    commonValidation(socialGraph, result);

    resultVertices.forEach(vertex -> assertTrue(
      "vertex " + vertex.getId() + " was not visited, all vertices should be",
      vertex.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean()));
  }

  /**
   * Test with social graph, with 3 starting vertices and all of the vertices to visit.
   */
  @Test
  public void visitAll3StartVerticesTest() throws Exception {
    LogicalGraph result = new KRandomJumpGellyVCI(3, 1000, 0.15,
      1.0).execute(socialGraph);

    commonValidation(socialGraph, result);

    resultVertices.forEach(vertex -> assertTrue(
      "vertex " + vertex.getId() + " was not visited, all vertices should be",
      vertex.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean()));
  }

  /**
   * Test with unconnected custom graph, with 1 starting vertex and all of the vertices to visit.
   */
  @Test
  public void visitAllJumpsOnlyTest() throws Exception {
    LogicalGraph result = new KRandomJumpGellyVCI(1, 1000, 0.15,
      1.0).execute(customGraph);

    commonValidation(customGraph, result);

    resultVertices.forEach(vertex -> assertTrue(
      "vertex " + vertex.getId() + " was not visited, all vertices should be",
      vertex.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean()));
  }

  /**
   * Validation for all test-cases. Writes the output, compares the vertex- and edge-count and
   * checks the annotation with the visited-property.
   *
   * @param graph The original graph
   * @param resultGraph The annotated result graph
   */
  private void commonValidation(LogicalGraph graph, LogicalGraph resultGraph) throws Exception {
    resultVertices = new ArrayList<>();
    List<EPGMEdge> resultEdges = new ArrayList<>();
    resultGraph.getVertices().output(new LocalCollectionOutputFormat<>(resultVertices));
    resultGraph.getEdges().output(new LocalCollectionOutputFormat<>(resultEdges));
    getExecutionEnvironment().execute();

    assertEquals("wrong number of vertices in resultGraph",
      graph.getVertices().count(), resultGraph.getVertices().count());
    assertEquals("wrong number of edges in resultGraph",
      graph.getEdges().count(), resultGraph.getEdges().count());
    resultVertices.forEach(vertex -> assertTrue("vertex " + vertex.getId() + " is not annotated",
      vertex.hasProperty(SamplingConstants.PROPERTY_KEY_SAMPLED)));
    resultEdges.forEach(edge -> assertTrue("edge " + edge.getId() + " is not annotated",
      edge.hasProperty(SamplingConstants.PROPERTY_KEY_SAMPLED)));

    for (EPGMEdge edge : resultEdges) {
      if (edge.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean()) {
        resultVertices.stream().filter(vertex -> vertex.getId().equals(edge.getSourceId())).forEach(
          sourceVertex -> assertTrue("source of visited edge is not visited",
            sourceVertex.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean()));
        resultVertices.stream().filter(vertex -> vertex.getId().equals(edge.getTargetId())).forEach(
          targetVertex -> assertTrue("target of visited edge is not visited",
            targetVertex.getPropertyValue(SamplingConstants.PROPERTY_KEY_SAMPLED).getBoolean()));
      }
    }
  }
}
