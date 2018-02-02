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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexRandomFilter;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 */
public class RandomNodeSampling implements UnaryGraphToGraphOperator {
  /**
   * relative amount of nodes in the result graph
   */
  private final float sampleSize;

  /**
   * seed for the random number generator
   * if no seed is null, the random generator is created without seed
   */
  private final long randomSeed;

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param sampleSize relative sample size
   */
  public RandomNodeSampling(float sampleSize) {
    this(sampleSize, 0L);
  }

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed value (can be {@code null})
   */
  public RandomNodeSampling(float sampleSize, long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    DataSet<Vertex> newVertices = graph.getVertices()
      .filter(new VertexRandomFilter<>(sampleSize, randomSeed));

    DataSet<Edge> newEdges = graph.getEdges()
      .join(newVertices)
      .where(new SourceId<>())
      .equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>())
      .join(newVertices)
      .where(new TargetId<>())
      .equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>());

    return graph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices, newEdges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return RandomNodeSampling.class.getName();
  }
}
