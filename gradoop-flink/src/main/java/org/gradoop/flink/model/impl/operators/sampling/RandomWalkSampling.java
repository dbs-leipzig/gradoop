/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.algorithms.gelly.randomjump.KRandomJumpGellyVCI;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.ByProperty;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingConstants;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeSourceVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgeTargetVertexJoin;
import org.gradoop.flink.model.impl.operators.sampling.functions.EdgesWithSampledVerticesFilter;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;

/**
 * Computes a random walk sampling of the graph (new graph head will be generated). Retains visited
 * vertices and edges where source and target vertex has been sampled.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class RandomWalkSampling<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends SamplingAlgorithm<G, V, E, LG, GC> {

  /**
   * Sample size
   */
  private final float sampleSize;
  /**
   * Number of start vertices
   */
  private final int numberOfStartVertices;
  /**
   * Probability of jumping instead of walking along edges
   */
  private final float jumpProbability;
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
  public RandomWalkSampling(float sampleSize, int numberOfStartVertices) {
    this.sampleSize = sampleSize;
    this.numberOfStartVertices = numberOfStartVertices;
    this.jumpProbability = 0.1f;
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
  public RandomWalkSampling(float sampleSize, int numberOfStartVertices, float jumpProbability,
    int maxIteration) {

    this.sampleSize = sampleSize;
    this.numberOfStartVertices = numberOfStartVertices;
    this.jumpProbability = jumpProbability;
    this.maxIteration = maxIteration;
  }

  @Override
  protected LG sample(LG graph) {

    LG gellyResult = graph.callForGraph(
      new KRandomJumpGellyVCI<>(numberOfStartVertices, maxIteration, jumpProbability, sampleSize));

    DataSet<V> sampledVertices = gellyResult.getVertices()
      .filter(new ByProperty<>(SamplingConstants.PROPERTY_KEY_SAMPLED));

    DataSet<E> sampledEdges = graph.getEdges()
      .join(sampledVertices)
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new EdgeSourceVertexJoin<>(SamplingConstants.PROPERTY_KEY_SAMPLED))
      .join(sampledVertices)
      .where(1).equalTo(new Id<>())
      .with(new EdgeTargetVertexJoin<>(SamplingConstants.PROPERTY_KEY_SAMPLED))
      .filter(new EdgesWithSampledVerticesFilter<>(Neighborhood.BOTH))
      .map(new Value0Of3<>());

    return graph.getFactory().fromDataSets(sampledVertices, sampledEdges);
  }
}
