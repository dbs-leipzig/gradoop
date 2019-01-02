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
package org.gradoop.flink.algorithms.gelly.randomjump;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for derived test classes for the RandomJump algorithms.
 */
@RunWith(Parameterized.class)
public abstract class RandomJumpBaseTest extends GradoopFlinkTestBase {

  /**
   * Enum to declare the used RandomJump algorithm
   */
  private enum RandomJumpAlgorithm {
    /**
     * VertexCentricIteration
     */
    VCI,
    /**
     * DataSetIteration
     */
    DSI
  }

  /**
   * Name for test-case
   */
  private String testName;

  /**
   * Value for maximum number of iterations for the algorithm
   */
  private final int maxIterations;

  /**
   * Probability for jumping to a random vertex instead of walking to a random neighbor
   */
  private final double jumpProbability;

  /**
   * Relative amount of vertices to visit via walk or jump
   */
  private final double percentageVisited;

  /**
   * The used RandomJump algorithm
   */
  private final RandomJumpAlgorithm algorithm;

  /**
   * Number of start vertices
   */
  private final int k;

  /**
   * The original graph used for testing
   */
  private LogicalGraph graph;

  /**
   * The result graph used for testing
   */
  private LogicalGraph resultGraph;

  /**
   * List for result vertices
   */
  private List<Vertex> resultVertices;

  /**
   * List for result edges
   */
  private List<Edge> resultEdges;

  /**
   * Creates an instance of the base test class.
   *
   * @param testName Name for test-case
   * @param algorithm The used RandomJump algorithm, determined via {@link RandomJumpAlgorithm}
   * @param k Number of start vertices
   * @param maxIterations Value for maximum number of iterations for the algorithm
   * @param jumpProbability Probability for jumping to a random vertex instead of walking to
   *                        a random neighbor
   * @param percentageVisited Relative amount of vertices to visit via walk or jump
   */
  public RandomJumpBaseTest(String testName, String algorithm, String k, String maxIterations,
    String jumpProbability, String percentageVisited) {
    this.testName = testName;
    this.algorithm = RandomJumpAlgorithm.valueOf(algorithm);
    this.k = Integer.parseInt(k);
    this.maxIterations = Integer.parseInt(maxIterations);
    this.jumpProbability = Double.parseDouble(jumpProbability);
    this.percentageVisited = Double.parseDouble(percentageVisited);
  }

  /**
   * Initialize graph for testing
   */
  @Before
  public void initGraph() throws Exception {

    if (testName.contains("visitAllJumpsOnly")) {
      String graphString = "graph[" +
        "/* no edges graph */" +
        "(v0 {id:0, value:\"A\"})" +
        "(v1 {id:1, value:\"B\"})" +
        "(v2 {id:2, value:\"C\"})" +
        "]";
      graph = getLoaderFromString(graphString).getLogicalGraphByVariable("graph");
    } else {
      graph = getSocialNetworkLoader().getLogicalGraph();
    }

    RandomJumpBase randomJump;
    if (algorithm == RandomJumpAlgorithm.VCI) {
      randomJump = new KRandomJumpGellyVCI(k, maxIterations, jumpProbability, percentageVisited);
    } else if (algorithm == RandomJumpAlgorithm.DSI) {
      randomJump = new KRandomJumpDataSetIteration(k, maxIterations, jumpProbability,
        percentageVisited);
    } else {
      throw new IllegalArgumentException("Unknown kind of RandomJump algorithm - use VCI or DSI");
    }
    resultGraph = randomJump.execute(graph);

    resultVertices = new ArrayList<>();
    resultEdges = new ArrayList<>();

    resultGraph.getVertices().output(new LocalCollectionOutputFormat<>(resultVertices));
    resultGraph.getEdges().output(new LocalCollectionOutputFormat<>(resultEdges));

    getExecutionEnvironment().execute();
  }

  /**
   * Check if all graph elements are annotated
   */
  @Test
  public void validateAnnotation() throws Exception {

    assertEquals("vertices are missing in resultGraph", graph.getVertices().count(),
      resultGraph.getVertices().count());
    assertEquals("edges are missing in resultGraph", graph.getEdges().count(),
      resultGraph.getEdges().count());

    resultVertices.forEach(vertex -> assertTrue(
      "vertex " + vertex.getId() + " is not annotated",
      vertex.hasProperty(RandomJumpBase.PROPERTY_KEY_VISITED)));

    resultEdges.forEach(edge -> assertTrue("edge " + edge.getId() + " is not annotated",
      edge.hasProperty(RandomJumpBase.PROPERTY_KEY_VISITED)));
  }

  /**
   * Check if the correct number of elements are visited
   */
  @Test
  public void checkVisitedProperty() {

    for (Edge edge : resultEdges)  {
      if (edge.getPropertyValue(RandomJumpBase.PROPERTY_KEY_VISITED).getBoolean()) {
        resultVertices.stream().filter(vertex -> vertex.getId().equals(edge.getSourceId()))
          .forEach(sourceVertex -> assertTrue("source of visited edge is not visited",
            sourceVertex.getPropertyValue(RandomJumpBase.PROPERTY_KEY_VISITED).getBoolean()));
        resultVertices.stream().filter(vertex -> vertex.getId().equals(edge.getTargetId()))
          .forEach(targetVertex -> assertTrue("target of visited edge is not visited",
            targetVertex.getPropertyValue(RandomJumpBase.PROPERTY_KEY_VISITED).getBoolean()));
      }
    }

    switch (testName) {
    case "base": {
      long visitedVertices = resultVertices.stream().filter(
        vertex -> vertex.getPropertyValue(RandomJumpBase.PROPERTY_KEY_VISITED)
          .getBoolean()).count();
      assertEquals("Wrong number of visited vertices, should be 6", 6L,
        visitedVertices);
      break;
    }
    case "base3StartVertices": {
      long visitedVertices = resultVertices.stream()
        .filter(vertex -> vertex.getPropertyValue(RandomJumpBase.PROPERTY_KEY_VISITED)
          .getBoolean()).count();
      assertEquals("Wrong number of visited vertices, should be 6", 6L,
        visitedVertices);
      break;
    }
    case "visitOne": {
      long visitedVertices = resultVertices.stream().filter(
        vertex -> vertex.getPropertyValue(RandomJumpBase.PROPERTY_KEY_VISITED)
          .getBoolean()).count();
      assertEquals("Wrong number of visited vertices, should be 1", 1L,
        visitedVertices);
      break;
    }
    default:
      resultVertices.forEach(vertex -> assertTrue(
        "vertex " + vertex.getId() + " was not visited, all vertices should be",
        vertex.getPropertyValue(RandomJumpBase.PROPERTY_KEY_VISITED).getBoolean()));
      break;
    }
  }
}
