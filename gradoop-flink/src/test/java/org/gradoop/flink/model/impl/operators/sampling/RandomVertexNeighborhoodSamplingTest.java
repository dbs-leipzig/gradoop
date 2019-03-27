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

import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.junit.runners.Parameterized;

import java.util.Arrays;

public class RandomVertexNeighborhoodSamplingTest extends ParameterizedTestForGraphSampling {

  /**
   * Creates a new RandomVertexNeighborhoodSamplingTest instance.
   *
   * @param testName Name for test-case
   * @param seed Seed-value for random number generator, e.g. 0
   * @param sampleSize Value for sample size, e.g. 0.5
   * @param neighborType The vertex neighborhood type, e.g. Neighborhood.BOTH
   */
  public RandomVertexNeighborhoodSamplingTest(String testName, String seed, String sampleSize,
                                              String neighborType) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize),
      Neighborhood.valueOf(neighborType));
  }

  @Override
  public SamplingAlgorithm getSamplingOperator() {
    return new RandomVertexNeighborhoodSampling(sampleSize, seed, neighborType);
  }

  @Override
  public void validateSpecific(LogicalGraph input, LogicalGraph output) {
  }


  /**
   * Parameters called when running the test
   *
   * @return List of parameters
   */
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new String[] {
      "VertexNeighborhoodSamplingTest with seed and both neighborhood",
      "-4181668494294894490",
      "0.272f",
      "BOTH"
    }, new String[] {
      "VertexNeighborhoodSamplingTest without seed and both neighborhood",
      "0",
      "0.272f",
      "BOTH"
    }, new String[] {
      "VertexNeighborhoodSamplingTest with seed and input neighborhood",
      "-4181668494294894490",
      "0.272f",
      "IN"
    }, new String[] {
      "VertexNeighborhoodSamplingTest with seed and output neighborhood",
      "-4181668494294894490",
      "0.272f",
      "OUT"
    });
  }
}
