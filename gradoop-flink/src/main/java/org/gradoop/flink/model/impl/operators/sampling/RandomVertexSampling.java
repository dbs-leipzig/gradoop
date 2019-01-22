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
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.sampling.functions.RandomFilter;

/**
 * Computes a vertex sampling of the graph. Retains randomly chosen vertices of a given relative
 * amount. Retains all edges which source- and target-vertices were chosen. There may retain some
 * unconnected vertices in the sampled graph.
 */
public class RandomVertexSampling extends SamplingAlgorithm {
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
  public LogicalGraph sample(LogicalGraph graph) {

    DataSet<Vertex> newVertices = graph.getVertices()
      .filter(new RandomFilter<>(sampleSize, randomSeed));

    DataSet<Edge> newEdges = graph.getEdges()
      .join(newVertices)
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new LeftSide<>())
      .join(newVertices)
      .where(new TargetId<>()).equalTo(new Id<>())
      .with(new LeftSide<>());

    return graph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices, newEdges);
  }

  @Override
  public String getName() {
    return RandomVertexSampling.class.getName();
  }
}
