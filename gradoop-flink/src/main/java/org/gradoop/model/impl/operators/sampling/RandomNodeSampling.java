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

package org.gradoop.model.impl.operators.sampling;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.utils.LeftSide;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.operators.sampling.functions.VertexRandomFilter;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class RandomNodeSampling<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {
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
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    DataSet<V> newVertices = graph.getVertices()
      .filter(new VertexRandomFilter<V>(sampleSize, randomSeed));

    DataSet<E> newEdges = graph.getEdges()
      .join(newVertices)
      .where(new SourceId<E>())
      .equalTo(new Id<V>())
      .with(new LeftSide<E, V>())
      .join(newVertices)
      .where(new TargetId<E>())
      .equalTo(new Id<V>())
      .with(new LeftSide<E, V>());

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
