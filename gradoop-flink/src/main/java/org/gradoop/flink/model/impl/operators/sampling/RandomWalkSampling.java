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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.randomjump.KRandomJumpGellyVCI;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeSourceVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeTargetVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgesWithSampledVerticesFilter;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;

/**
 * Computes a random walk sampling of the graph. Retains visited vertices and edges where source
 * and target vertex has been sampled.
 */
public class RandomWalkSampling extends SamplingAlgorithm {

  /**
   * Sample size
   */
  private final double sampleSize;
  /**
   * Number of start vertices
   */
  private final int numberOfStartVertices;
  /**
   * Probability of jumping instead of walking along edges
   */
  private final double jumpProbability;
  /**
   * Max iteration count
   */
  private final int maxIteration;


  /**
   * Constructor to create an instance of RandomWalk sampling
   *
   * @param sampleSize              sample size
   * @param numberOfStartVertices   number of start vertices
   */
  public RandomWalkSampling(double sampleSize, int numberOfStartVertices) {
    this.sampleSize = sampleSize;
    this.numberOfStartVertices = numberOfStartVertices;
    this.jumpProbability = 0.1d;
    this.maxIteration = Integer.MAX_VALUE;
  }

  /**
   * Constructor to create an instance of RandomWalk sampling
   *
   * @param sampleSize              sample size
   * @param numberOfStartVertices   number of start vertices
   * @param jumpProbability         probability to jump instead of walk
   * @param maxIteration            max gelly iteration count
   */
  public RandomWalkSampling(double sampleSize, int numberOfStartVertices,
    double jumpProbability, int maxIteration) {

    this.sampleSize = sampleSize;
    this.numberOfStartVertices = numberOfStartVertices;
    this.jumpProbability = jumpProbability;
    this.maxIteration = maxIteration;
  }

  @Override
  protected LogicalGraph sample(LogicalGraph graph) {

    LogicalGraph gellyResult = new KRandomJumpGellyVCI(numberOfStartVertices, maxIteration,
      jumpProbability, sampleSize).execute(graph);

    DataSet<Vertex> sampledVertices = gellyResult.getVertices()
      .filter(new ByProperty<>(PROPERTY_KEY_SAMPLED));

    DataSet<Edge> sampledEdges = graph.getEdges()
      .join(sampledVertices)
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new EdgeSourceVertexJoin(PROPERTY_KEY_SAMPLED))
      .join(sampledVertices)
      .where(1).equalTo(new Id<>())
      .with(new EdgeTargetVertexJoin(PROPERTY_KEY_SAMPLED))
      .filter(new EdgesWithSampledVerticesFilter(Neighborhood.BOTH))
      .map(new Value0Of3<>());

    return graph.getFactory().fromDataSets(sampledVertices, sampledEdges);
  }
}
