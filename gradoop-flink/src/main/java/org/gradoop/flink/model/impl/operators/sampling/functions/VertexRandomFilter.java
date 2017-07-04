/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Random;

/**
 * Creates a random value for each vertex and filters those that are below a
 * given threshold.
 *
 * @param <V> EPGM vertex type
 */
public class VertexRandomFilter<V extends Vertex>
  implements FilterFunction<V> {
  /**
   * Threshold to decide if a vertex needs to be filtered.
   */
  private final float threshold;
  /**
   * Random instance
   */
  private final Random randomGenerator;

  /**
   * Creates a new filter instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed (can be {@code} null)
   */
  public VertexRandomFilter(float sampleSize, long randomSeed) {
    threshold = sampleSize;
    randomGenerator =
      (randomSeed != 0L) ? new Random(randomSeed) : new Random();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(V vertex) throws Exception {
    return randomGenerator.nextFloat() < threshold;
  }
}
