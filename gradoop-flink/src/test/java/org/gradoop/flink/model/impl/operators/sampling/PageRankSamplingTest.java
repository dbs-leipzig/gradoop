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
package org.gradoop.flink.model.impl.operators.sampling;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingConstants;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Parameterized test-class for {@link PageRankSampling}.
 */
public class PageRankSamplingTest extends ParameterizedTestForGraphSampling {

  /**
   * Creates a new PageRankSamplingTest instance, parsing the parameters.
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   * @param dampeningFactor The dampening factor used by Flinks PageRank-algorithm, e.g. 0.85
   * @param maxIteration The iteration number used by Flinks PageRank-algorithm, e.g. 20
   * @param sampleGreaterThanThreshold Whether to sample vertices with a PageRank-score
   *                                   greater (true) or equal/smaller (false) the threshold
   * @param keepVerticesIfSameScore Whether to sample all vertices (true) or none of them (false)
   *                                in case all vertices got the same PageRank-score.
   */
  public PageRankSamplingTest(String testName, String seed, String sampleSize,
    String dampeningFactor, String maxIteration, String sampleGreaterThanThreshold,
    String keepVerticesIfSameScore) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize),
      Double.parseDouble(dampeningFactor), Integer.parseInt(maxIteration),
      Boolean.parseBoolean(sampleGreaterThanThreshold),
      Boolean.parseBoolean(keepVerticesIfSameScore));
  }

  @Override
  public SamplingAlgorithm getSamplingOperator() {
    return new PageRankSampling(dampeningFactor, maxIteration, (double) sampleSize,
      sampleGreaterThanThreshold, keepVerticesIfSameScore);
  }

  @Override
  public void validateSpecific(LogicalGraph input, LogicalGraph output) {

    boolean allHaveScore = true;
    boolean noneHasScore = true;

    for (Vertex v : newVertices) {
      if (v.hasProperty(SamplingConstants.SCALED_PAGE_RANK_SCORE_PROPERTY_KEY)) {
        noneHasScore = false;
      } else {
        allHaveScore = false;
      }
    }

    assertTrue("some vertices do and some do not have scaled PageRank-score property",
      noneHasScore || allHaveScore);

    if (newVertices.isEmpty()) {
      // Result is empty, if input is empty
      // OR all have the same score and keepVerticesIfSameScore = false
      assertTrue("some vertices got sampled (should NOT be)",
        dbVertices.isEmpty() || !keepVerticesIfSameScore);
    } else if (allHaveScore) { // normal case
      for (Vertex v : newVertices) {
        double score = v.getPropertyValue(SamplingConstants.SCALED_PAGE_RANK_SCORE_PROPERTY_KEY)
          .getDouble();
        if (sampleGreaterThanThreshold) {
          assertTrue("sampled vertex has PageRankScore smaller or equal than threshold",
            score > sampleSize);
        } else {
          assertTrue("sampled vertex has PageRankScore greater than threshold",
            score <= sampleSize);
        }
      }
    } else if (keepVerticesIfSameScore) { // and noneHasScore
      assertEquals("not all vertices got sampled (should be, all got same score)",
        dbVertices.size(), newVertices.size());
    }
    dbEdges.removeAll(newEdges);
    for (Edge edge : dbEdges) {
      assertFalse("edge from original graph was not sampled but source and target were",
        newVertexIDs.contains(edge.getSourceId()) &&
          newVertexIDs.contains(edge.getTargetId()));
    }
  }

  /**
   * Test special graph with the same page rank score for each vertex.
   *
   * @throws Exception on failure
   */
  @Test
  public void testGraphWithSameScore() throws Exception {
    // test special graph
    LogicalGraph specialGraph = getLoaderFromString(
      "(alice:Person {name : \"Alice\"})\n" +
        "(eve:Person {name : \"Eve\"})\n" +
        "g0:Community {interest : \"Friends\", vertexCount : 2} [\n" +
        "(eve)-[eka:knows {since : 2013}]->(alice)\n" +
        "(alice)-[ake:knows {since : 2013}]->(eve)]").getLogicalGraphByVariable("g0");

    LogicalGraph sampledGraph = getSamplingOperator().sample(specialGraph);

    validateGraph(specialGraph, sampledGraph);
    validateSpecific(specialGraph, sampledGraph);
  }

  /**
   * Parameters called when running the test
   *
   * @return List of parameters
   */
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new String[] {
      "PageRankSamplingTest with seed, sample vertices with PageRankScore greater than " +
        "threshold, keep all vertices if they got same score",
      "-4181668494294894490",
      "0.272f",
      "0.5f",
      "40",
      "true",
      "true"
    }, new String[] {
      "PageRankSamplingTest without seed, sample vertices with PageRankScore greater than " +
        "threshold, keep all vertices if they got same score",
      "0",
      "0.272f",
      "0.5f",
      "40",
      "true",
      "true"
    }, new String[] {
      "PageRankSamplingTest without seed, sample vertices with PageRankScore equal/smaller " +
        "than threshold, keep all vertices if they got same score",
      "0",
      "0.272f",
      "0.5f",
      "40",
      "false",
      "true"
    }, new String[] {
      "PageRankSamplingTest without seed, sample vertices with PageRankScore equal/smaller " +
        "than threshold, keep no vertices if they got same score",
      "0",
      "0.272f",
      "0.5f",
      "40",
      "false",
      "false"
    }, new String[] {
      "PageRankSamplingTest without seed and sampled vertices with PageRankScore equal/smaller " +
        "than threshold, iteration = 1, keep all vertices if they got same score",
      "0",
      "0.272f",
      "0.5f",
      "1",
      "false",
      "true"
    });
  }
}
