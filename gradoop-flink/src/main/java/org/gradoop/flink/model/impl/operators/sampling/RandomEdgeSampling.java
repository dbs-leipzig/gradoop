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
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.sampling.functions.RandomFilter;

/**
 * Computes an edge sampling of the graph. Retains randomly chosen edges of a given relative amount
 * and their associated source- and target-vertices. No unconnected vertices will retain in the
 * sampled graph.
 */
public class RandomEdgeSampling extends SamplingAlgorithm {
  /**
   * Relative amount of edges in the result graph
   */
  private final float sampleSize;

  /**
   * Seed for the random number generator
   * If seed is 0, the random generator is created without seed
   */
  private final long randomSeed;

  /**
   * Creates new RandomEdgeSampling instance.
   *
   * @param sampleSize relative sample size, e.g. 0.5
   */
  public RandomEdgeSampling(float sampleSize) {
    this(sampleSize, 0L);
  }

  /**
   * Creates new RandomEdgeSampling instance.
   *
   * @param sampleSize relative sample size, e.g. 0.5
   * @param randomSeed random seed value (can be 0)
   */
  public RandomEdgeSampling(float sampleSize, long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph sample(LogicalGraph graph) {
    DataSet<Edge> newEdges = graph.getEdges().filter(new RandomFilter<>(sampleSize, randomSeed));

    DataSet<Vertex> newSourceVertices = graph.getVertices()
      .join(newEdges)
      .where(new Id<>()).equalTo(new SourceId<>())
      .with(new LeftSide<>())
      .distinct(new Id<>());

    DataSet<Vertex> newTargetVertices = graph.getVertices()
      .join(newEdges)
      .where(new Id<>()).equalTo(new TargetId<>())
      .with(new LeftSide<>())
      .distinct(new Id<>());

    DataSet<Vertex> newVertices = newSourceVertices.union(newTargetVertices).distinct(new Id<>());
    return graph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices, newEdges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return RandomEdgeSampling.class.getName();
  }
}
