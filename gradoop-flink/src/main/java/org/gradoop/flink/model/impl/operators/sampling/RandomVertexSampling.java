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
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.operators.sampling.functions.RandomFilter;

/**
 * Computes a vertex sampling of the graph (new graph head will be generated). Retains randomly
 * chosen vertices of a given relative amount. Retains all edges which source- and
 * target-vertices were chosen. There may retain some unconnected vertices in the sampled graph.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class RandomVertexSampling<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends SamplingAlgorithm<G, V, E, LG, GC> {
  /**
   * Relative amount of nodes in the result graph
   */
  private final float sampleSize;

  /**
   * Seed for the random number generator
   * If no seed is null, the random generator is created without seed
   */
  private final long randomSeed;

  /**
   * Creates new RandomVertexSampling instance.
   *
   * @param sampleSize relative sample size
   */
  public RandomVertexSampling(float sampleSize) {
    this(sampleSize, 0L);
  }

  /**
   * Creates new RandomVertexSampling instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed value (can be {@code null})
   */
  public RandomVertexSampling(float sampleSize, long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
  }

  @Override
  public LG sample(LG graph) {
    DataSet<V> newVertices = graph.getVertices().filter(new RandomFilter<>(sampleSize, randomSeed));
    return graph.getFactory().fromDataSets(newVertices, graph.getEdges()).verify();
  }
}
