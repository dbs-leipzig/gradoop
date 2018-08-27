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
package org.gradoop.flink.model.impl.operators.sampling;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;

public class RandomVertexSamplingTest extends ParametrizedTestForGraphSampling {

  /**
   * Creates a new RandomVertexSamplingTest instance.
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   */
  public RandomVertexSamplingTest(String testName, String seed, String sampleSize) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SamplingAlgorithm getSamplingOperator() {
    return new RandomVertexSampling(sampleSize, seed);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void validateSpecific(LogicalGraph input, LogicalGraph output) {

    dbEdges.removeAll(newEdges);
    for (Edge edge : dbEdges) {
      assertFalse("edge from original graph was not sampled but source and target were",
        newVertexIDs.contains(edge.getSourceId()) &&
          newVertexIDs.contains(edge.getTargetId()));
    }
  }

  /**
   * Parameters called when running the test
   *
   * @return List of parameters
   */
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "VertexSamplingTest with seed",
        "-4181668494294894490",
        "0.272f"
      },
      new String[] {
        "VertexSamplingTest without seed",
        "0",
        "0.272f"
      });
  }
}
