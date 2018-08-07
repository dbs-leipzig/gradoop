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
 * Creates a random value for each vertex and retains those that are below a given threshold.
 * A degree-dependent value is taken into account to have a bias towards high-degree vertices.
 * The assumption is that a property of vertices degree and max degree.
 *
 * @param <V> EPGM vertex type
 */
public class NonUniformVertexRandomFilter<V extends Vertex>
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
   * Property name which has the degree of the vertex
   */
  private String propertyNameOfDegree;
  /**
   * Property name which has the degree of the vertex
   */
  private String propertyNameofMaxDegree;

  /**
   * Creates a new filter instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed (can be 0)
   * @param propertyNameOfDegree proeprty name of degree
   * @param propertyNameofMaxDegree property name of max degree
   */
  public NonUniformVertexRandomFilter(float sampleSize, long randomSeed,
    String propertyNameOfDegree, String propertyNameofMaxDegree) {
    threshold = sampleSize;
    randomGenerator = (randomSeed != 0L) ? new Random(randomSeed) : new Random();
    this.propertyNameOfDegree = propertyNameOfDegree;
    this.propertyNameofMaxDegree = propertyNameofMaxDegree;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(V vertex) throws Exception {
    long degree = Long.parseLong(vertex.getPropertyValue(propertyNameOfDegree).toString());
    long maxDegree = Long.parseLong(vertex.getPropertyValue(propertyNameofMaxDegree).toString());
    return randomGenerator.nextFloat() <= (degree / (double) maxDegree) * threshold;
  }
}
