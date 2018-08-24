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

import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.junit.runners.Parameterized;

import java.util.Arrays;

public class RandomVertexEdgeSamplingTest extends ParametrizedTestForGraphSampling {

  /**
   * Creates a new RandomVertexEdgeSamplingTest instance.
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for vertex sample size, e.g. 0.5
   * @param edgeSampleSize Value for edge sample size, e.g. 0.5
   * @param vertexEdgeSamplingType Type for VertexEdgeSampling, e.g. SimpleVersion
   */
  public RandomVertexEdgeSamplingTest(String testName, String seed, String sampleSize,
    String edgeSampleSize, String vertexEdgeSamplingType) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize), Float.parseFloat(edgeSampleSize),
      RandomVertexEdgeSampling.VertexEdgeSamplingType.valueOf(vertexEdgeSamplingType));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SamplingAlgorithm getSamplingOperator() {
    return new RandomVertexEdgeSampling(sampleSize, edgeSampleSize, seed, vertexEdgeSamplingType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void validateSpecific(LogicalGraph input, LogicalGraph output) {}

  /**
   * Parameters called when running the test
   *
   * @return List of parameters
   */
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "VertexEdgeSamplingTest with seed and simple version",
        "-4181668494294894490",
        "0.272f",
        "0.272f",
        "SimpleVersion"
      },
      new String[] {
        "VertexEdgeSamplingTest without seed and simple version",
        "0",
        "0.272f",
        "0.272f",
        "SimpleVersion"
      },
      new String[] {
        "VertexEdgeSamplingTest without seed and nonuniform version",
        "0",
        "0.272f",
        "0.272f",
        "NonuniformVersion"
      },
      new String[] {
        "VertexEdgeSamplingTest without seed and nonuniform hybrid version",
        "0",
        "0.272f",
        "0.272f",
        "NonuniformHybridVersion"
      });
  }
}
