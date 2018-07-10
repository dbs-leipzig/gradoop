/**
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

import java.security.InvalidParameterException;

/**
 * Computes an edge sampling of the graph. First selects randomly chosen vertices of a given
 * relative amount and all edges which source- and target-vertices where chosen. Then randomly
 * chooses edges from this set of edges and their associated source- and target-vertices.
 * No unconnected vertices will retain in the sampled graph.
 */
public class RandomVertexEdgeSampling implements UnaryGraphToGraphOperator {

  /**
   * The samping type enum
   */
  public enum VertexEdgeSamplingType {
    /**
     * Simple version (uniform version for both vertices and edges
     */
    SimpleVersion,
    /**
     * Nonuniform vertex sampling and then uniform edge sampling
     */
    NonuniformVersion,
    /**
     * Nonuniform vertex sampling and sample sizes add up to 1
     */
    NonuniformHybridVersion,
  }

  /**
   * The sampling type
   */
  private final VertexEdgeSamplingType vertexEdgeSamplingType;

  /**
   * Relative amount of vertices in the result graph
   */
  private final float vertexSampleSize;

  /**
   * Relative amount of edges in the result graph
   */
  private final float edgeSampleSize;

  /**
   * Seed for the random number generator
   * If seed is 0, the random generator is created without seed
   */
  private final long randomSeed;

  /**
   * Creates new RandomVertexEdgeSampling instance.
   *
   * @param sampleSize relative sample size for edges and vertices
   */
  public RandomVertexEdgeSampling(float sampleSize) {
    this(sampleSize, sampleSize, 0L, VertexEdgeSamplingType.SimpleVersion);
  }

  /**
   * Creates new RandomVertexEdgeSampling instance.
   *
   * @param vertexSampleSize relative sample size for vertices
   * @param edgeSampleSize relative sample size for edges
   */
  public RandomVertexEdgeSampling(float vertexSampleSize, float edgeSampleSize) {
    this(vertexSampleSize, edgeSampleSize, 0L, VertexEdgeSamplingType.SimpleVersion);
  }

  /**
   * Creates new RandomVertexEdgeSampling instance.
   *
   * @param vertexSampleSize relative sample size for vertices
   * @param edgeSampleSize relative sample size for edges
   * @param vertexEdgeSamplingType the type of sampling
   */
  public RandomVertexEdgeSampling(float vertexSampleSize, float edgeSampleSize,
                                  VertexEdgeSamplingType vertexEdgeSamplingType) {
    this(vertexSampleSize, edgeSampleSize, 0L, vertexEdgeSamplingType);
  }

  /**
   * Creates new RandomVertexEdgeSampling instance.
   *
   * @param vertexSampleSize relative sample size for vertices
   * @param edgeSampleSize relative sample size for edges
   * @param randomSeed random seed value (can be 0)
   * @param vertexEdgeSamplingType type of sampling
   */
  public RandomVertexEdgeSampling(float vertexSampleSize, float edgeSampleSize, long randomSeed,
                                  VertexEdgeSamplingType vertexEdgeSamplingType) {
    this.vertexSampleSize = vertexSampleSize;
    this.edgeSampleSize = edgeSampleSize;
    this.randomSeed = randomSeed;
    this.vertexEdgeSamplingType = vertexEdgeSamplingType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    if (vertexEdgeSamplingType == VertexEdgeSamplingType.SimpleVersion) {
      graph = new RandomVertexSampling(vertexSampleSize, randomSeed).execute(graph);
      graph = new RandomEdgeSampling(edgeSampleSize, randomSeed).execute(graph);
    } else if (vertexEdgeSamplingType == VertexEdgeSamplingType.NonuniformVersion ||
            vertexEdgeSamplingType == VertexEdgeSamplingType.NonuniformHybridVersion) {
      graph = new RandomNonUniformVertexSampling(vertexSampleSize, randomSeed).execute(graph);
      if (vertexEdgeSamplingType == VertexEdgeSamplingType.NonuniformHybridVersion) {
        graph = new RandomEdgeSampling(1 - vertexSampleSize, randomSeed).execute(graph);
      } else {
        graph = new RandomEdgeSampling(edgeSampleSize, randomSeed).execute(graph);
      }
    } else {
      throw new InvalidParameterException();
    }
    return graph;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return RandomVertexEdgeSampling.class.getName();
  }

  /**
   * Converts a string to sample type
   *
   * @param type the string containing the sample type
   * @return the actual type of sampling
   */
  public static VertexEdgeSamplingType sampleTypeFromString(String type) {
    VertexEdgeSamplingType vertexEdgeSamplingType;
    if (type.equals(RandomVertexEdgeSampling.VertexEdgeSamplingType.SimpleVersion.toString())) {
      vertexEdgeSamplingType = VertexEdgeSamplingType.SimpleVersion;
    } else if (type.equals(VertexEdgeSamplingType.NonuniformVersion.toString())) {
      vertexEdgeSamplingType = VertexEdgeSamplingType.NonuniformVersion;
    } else if (type.equals(VertexEdgeSamplingType.NonuniformHybridVersion.toString())) {
      vertexEdgeSamplingType = VertexEdgeSamplingType.NonuniformHybridVersion;
    } else {
      throw new InvalidParameterException();
    }
    return vertexEdgeSamplingType;
  }
}
