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
import org.junit.runners.Parameterized;

import java.util.Arrays;


public class RandomWalkSamplingTest extends ParameterizedTestForGraphSampling {

  public RandomWalkSamplingTest(String testName, String seed, String sampleSize) {
    super(testName, Long.parseLong(seed), Float.parseFloat(sampleSize));
  }

  @Override
  public SamplingAlgorithm getSamplingOperator() {
    return new RandomWalkSampling(sampleSize, numberOfStartVertices);
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
      "RandomWalkSamplingTest with seed",
      "-4181668494294894490",
      "0.272f"
    }, new String[] {
      "RandomWalkSamplingTest without seed",
      "0",
      "0.272f"
    });
  }
}
