/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Tests class {@link RandomEdgeSampling}.
 */
public class RandomEdgeSamplingTest extends ParameterizedTestForGraphSampling {

  /**
   * Creates a new RandomEdgeSamplingTest instance.
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   */
  public RandomEdgeSamplingTest(String testName, String seed, String sampleSize) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize));
  }

  @Override
  public SamplingAlgorithm<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>
  getSamplingOperator() {
    return new RandomEdgeSampling<>(sampleSize, seed);
  }

  @Override
  public void validateSpecific(LogicalGraph input, LogicalGraph output) {
    Set<GradoopId> connectedVerticesIDs = new HashSet<>();
    for (EPGMEdge edge : newEdges) {
      connectedVerticesIDs.add(edge.getSourceId());
      connectedVerticesIDs.add(edge.getTargetId());
    }
    newVertexIDs.removeAll(connectedVerticesIDs);
    assertTrue("there are unconnected vertices in the sampled graph", newVertexIDs.isEmpty());
  }

  /**
   * Parameters called when running the test
   *
   * @return List of parameters
   */
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable<String[]> data() {
    return Arrays.asList(new String[] {
      "EdgeSamplingTest with seed",
      "-4181668494294894490",
      "0.272f"
    }, new String[] {
      "EdgeSamplingTest without seed",
      "0",
      "0.272f"
    });
  }
}
