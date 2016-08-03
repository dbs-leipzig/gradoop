/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.sampling;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
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

    return LogicalGraph.fromDataSets(
      newVertices, newEdges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return RandomNodeSampling.class.getName();
  }
}
