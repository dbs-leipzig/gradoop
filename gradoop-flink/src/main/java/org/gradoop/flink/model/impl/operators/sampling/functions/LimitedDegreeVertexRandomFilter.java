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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Random;

/**
 * Retains all vertices with degrees greater than the degree threshold.
 * Also creates a random value for other vertices and retains those that are below the given
 * threshold for sampleSize.
 *
 * @param <V> EPGM vertex type
 */
public class LimitedDegreeVertexRandomFilter<V extends Vertex>
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
   * Degree Threshold
   */
  private final long degreeThreshold;

  /**
   * Type of vertex-degree to be considered in sampling:
   * input degree, output degree, sum of both.
   */
  private final VertexDegree degreeType;

  /**
   * Creates a new filter instance.
   *
   * @param sampleSize relative sample size, e.g. 0.5
   * @param randomSeed random seed (can be {@code} null)
   * @param degreeThreshold the threshold for the vertex degree, e.g. 3
   * @param degreeType the degree type considered for sampling, e.g. VertexDegree.IN
   */
  public LimitedDegreeVertexRandomFilter(float sampleSize, long randomSeed, long degreeThreshold,
    VertexDegree degreeType) {
    this.threshold = sampleSize;
    this.randomGenerator = (randomSeed != 0L) ? new Random(randomSeed) : new Random();
    this.degreeThreshold = degreeThreshold;
    this.degreeType = degreeType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(V vertex) throws Exception {
    long degree = Long.parseLong(vertex.getPropertyValue(degreeType.getName()).toString());

    if (degree > degreeThreshold) {
      return true;
    } else {
      return randomGenerator.nextFloat() <= threshold;
    }
  }
}
