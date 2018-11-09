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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

/**
 * Test class for {@link RandomJumpGellyVCI}, derived from {@link RandomJumpBaseTest}
 */
@RunWith(Parameterized.class)
public class RandomJumpGellyVCITest extends RandomJumpBaseTest {

  /**
   * Creates an instance of RandomJumpGellyVCITest.
   * Calls constructor of super-class.
   *
   * @param testName Name for test-case
   * @param algorithm The used RandomJump algorithm
   * @param maxIterations Value for maximum number of iterations for the algorithm
   * @param jumpProbability Probability for jumping to a random vertex instead of walking to
   *                        a random neighbor
   * @param percentageVisited Relative amount of vertices to visit via walk or jump
   */
  public RandomJumpGellyVCITest(String testName, String algorithm, String maxIterations,
    String jumpProbability, String percentageVisited) {
    super(testName, algorithm, maxIterations, jumpProbability, percentageVisited);
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
        "base", "VCI", "10000", "0.15", "0.5"
      }, new String[] {
        "visitOne", "VCI", "10000", "0.15", "0.09"
      }, new String[] {
        "visitAll", "VCI", "10000", "0.15", "1.0"
      }, new String[] {
        "visitAllJumpsOnly", "VCI", "10000", "0.15", "1.0"
      });
  }
}
