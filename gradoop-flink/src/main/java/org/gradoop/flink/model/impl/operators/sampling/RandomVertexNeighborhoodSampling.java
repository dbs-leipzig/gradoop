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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeWithSourceTarget;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexRandomMarkedMap;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexWithId;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeSourceVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeTargetVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgesWithSampledVerticesFilter;
import org.gradoop.flink.model.impl.operators.sampling.functions.FilterVerticesWithDegreeOtherThanGiven;

/**
 * Computes a vertex sampling of the graph. Retains randomly chosen vertices of a given relative
 * amount and includes all neighbors of those vertices in the sampling. All edges which source-
 * and target-vertices where chosen are sampled, too.
 */
public class RandomVertexNeighborhoodSampling extends SamplingAlgorithm {

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
   * Type of degree which should be considered: input degree, output degree, sum of both.
   */
  private final Neighborhood neighborType;

  /**
   * Creates new RandomVertexNeighborhoodSampling instance.
   *
   * @param sampleSize relative sample size
   */
  public RandomVertexNeighborhoodSampling(float sampleSize) {
    this(sampleSize, 0L);
  }

  /**
   * Creates new RandomVertexNeighborhoodSampling instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed value (can be 0)
   */
  public RandomVertexNeighborhoodSampling(float sampleSize, long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
    this.neighborType = Neighborhood.BOTH;
  }

  /**
   * Creates new RandomVertexNeighborhoodSampling instance.
   *
   * @param sampleSize   relative sample size
   * @param randomSeed   random seed value (can be 0
   * @param neighborType type of neighbor-vertex for sampling
   */
  public RandomVertexNeighborhoodSampling(float sampleSize, long randomSeed,
                                          Neighborhood neighborType) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
    this.neighborType = neighborType;
  }

  /**
   * Creates new RandomVertexSampling instance.
   *
   * @param sampleSize   relative sample size
   * @param neighborType type of neighbor-vertex for sampling
   */
  public RandomVertexNeighborhoodSampling(float sampleSize,
                                          Neighborhood neighborType) {
    this.sampleSize = sampleSize;
    this.randomSeed = 0L;
    this.neighborType = neighborType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph sample(LogicalGraph graph) {
    DataSet<Tuple2<Vertex, GradoopId>> sampledVerticesWithId = graph.getVertices()
      .map(new VertexRandomMarkedMap<>(sampleSize, randomSeed, PROPERTY_KEY_SAMPLED))
      .map(new VertexWithId());

    DataSet<Tuple3<Edge, GradoopId, GradoopId>> edgeSourceIdTargetId = graph.getEdges()
      .map(new EdgeWithSourceTarget());

    DataSet<Edge> newEdges = edgeSourceIdTargetId
      .join(sampledVerticesWithId)
      .where(1).equalTo(1)
      .with(new EdgeSourceVertexJoin())
      .join(sampledVerticesWithId)
      .where(2).equalTo(1)
      .with(new EdgeTargetVertexJoin())
      .filter(new EdgesWithSampledVerticesFilter(PROPERTY_KEY_SAMPLED, neighborType))
      .map(new Value0Of3<>());

    graph = graph.getConfig().getLogicalGraphFactory().fromDataSets(graph.getVertices(), newEdges);
    graph = new FilterVerticesWithDegreeOtherThanGiven(0L).execute(graph);

    return graph;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return RandomVertexNeighborhoodSampling.class.getName();
  }
}
