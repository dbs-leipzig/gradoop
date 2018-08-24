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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.PropertyRemover;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.sampling.functions.LimitedDegreeVertexRandomFilter;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;

/**
 * Computes a vertex sampling of the graph. Retains all vertices with a degree greater than a given
 * degree threshold and degree type. Also retains randomly chosen vertices with a degree smaller
 * or equal this threshold. Retains all edges which source- and target-vertices where chosen.
 */
public class RandomLimitedDegreeVertexSampling extends SamplingAlgorithm {

  /**
   * Relative amount of vertices in the result graph
   */
  private final float sampleSize;

  /**
   * Seed for the random number generator
   * If seed is 0, the random generator is created without seed
   */
  private final long randomSeed;

  /**
   * Threshold for the vertex degrees
   * All vertices with degrees higher this threshold will be sampled
   */
  private final long degreeThreshold;

  /**
   * Type of vertex degree to be considered in sampling:
   * input degree, output degree, sum of both.
   */
  private final VertexDegree degreeType;

  /**
   * Creates new RandomLimitedDegreeVertexSampling instance.
   *
   * @param sampleSize relative sample size, e.g. 0.5
   */
  public RandomLimitedDegreeVertexSampling(float sampleSize) {
    this(sampleSize, 0L);
  }

  /**
   * Creates new RandomLimitedDegreeVertexSampling instance.
   *
   * @param sampleSize relative sample size, e.g. 0.5
   * @param randomSeed random seed value (can be {@code null})
   */
  public RandomLimitedDegreeVertexSampling(float sampleSize, long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
    this.degreeThreshold = 2L;
    this.degreeType = VertexDegree.BOTH;
  }

  /**
   * Creates new RandomLimitedDegreeVertexSampling instance.
   *
   * @param sampleSize relative sample size, e.g. 0.5
   * @param randomSeed random seed value (can be {@code null}
   * @param degreeThreshold threshold for degrees of sampled vertices, e.g. 3
   * @param degreeType type of degree for sampling, e.g. VertexDegree.DegreeType.InputDegree
   */
  public RandomLimitedDegreeVertexSampling(float sampleSize, long randomSeed, long degreeThreshold,
    VertexDegree degreeType) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
    this.degreeThreshold = degreeThreshold;
    this.degreeType = degreeType;
  }

  /**
   * Creates new RandomLimitedDegreeVertexSampling instance.
   *
   * @param sampleSize relative sample size, e.g. 0.5
   * @param degreeThreshold threshold for degrees of sampled vertices, e.g. 3
   * @param degreeType type of degree for sampling, e.g. VertexDegree.DegreeType.InputDegree
   */
  public RandomLimitedDegreeVertexSampling(float sampleSize, long degreeThreshold,
    VertexDegree degreeType) {
    this.sampleSize = sampleSize;
    this.randomSeed = 0L;
    this.degreeThreshold = degreeThreshold;
    this.degreeType = degreeType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph sample(LogicalGraph graph) {

    graph = new DistinctVertexDegrees(
      DEGREE_PROPERTY_KEY,
      IN_DEGREE_PROPERTY_KEY,
      OUT_DEGREE_PROPERTY_KEY,
      true).execute(graph);

    DataSet<Vertex> newVertices = graph.getVertices()
      .filter(new LimitedDegreeVertexRandomFilter<>(
        sampleSize, randomSeed, degreeThreshold, degreeType))
      .map(new PropertyRemover<>(DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(IN_DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(OUT_DEGREE_PROPERTY_KEY));

    DataSet<Edge> newEdges = graph.getEdges()
      .join(newVertices)
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new LeftSide<>())
      .join(newVertices)
      .where(new TargetId<>()).equalTo(new Id<>())
      .with(new LeftSide<>());

    return graph.getConfig().getLogicalGraphFactory()
      .fromDataSets(graph.getGraphHead(), newVertices, newEdges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return RandomLimitedDegreeVertexSampling.class.getName();
  }

}
