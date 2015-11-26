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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.operators.logicalgraph.unary.sampling;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.joinfunctions.EdgeVertexJoinKeepEdge;
import org.gradoop.model.impl.functions.keyselectors.EdgeSourceVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors.EdgeTargetVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;

import java.util.Random;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public class RandomNodeSampling
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<V, E, G> {
  /**
   * relative amount of nodes in the result graph
   */
  private final Float sampleSize;

  /**
   * seed for the random number generator
   * if no seed is null, the random generator is created without seed
   */
  private final Long randomSeed;

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param sampleSize relative sample size
   */
  public RandomNodeSampling(Float sampleSize) {
    this(sampleSize, null);
  }

  /**
   * Creates new RandomNodeSampling instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed value (can be {@code null})
   */
  public RandomNodeSampling(Float sampleSize, Long randomSeed) {
    this.sampleSize = sampleSize;
    this.randomSeed = randomSeed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) throws
    Exception {

    DataSet<V> newVertices = graph.getVertices()
      .filter(new VertexRandomFilter<V>(sampleSize, randomSeed));

    DataSet<E> newEdges = graph.getEdges()
      .join(newVertices)
      .where(new EdgeSourceVertexKeySelector<E>())
      .equalTo(new VertexKeySelector<V>())
      .with(new EdgeVertexJoinKeepEdge<V, E>())
      .join(newVertices)
      .where(new EdgeTargetVertexKeySelector<E>())
      .equalTo(new VertexKeySelector<V>())
      .with(new EdgeVertexJoinKeepEdge<V, E>());

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

  /**
   * Creates a random value for each vertex and filters those that are below
   * a given threshold.
   *
   * @param <VD> vertex data type
   */
  private static class VertexRandomFilter<VD extends EPGMVertex>
    implements FilterFunction<VD> {
    /**
     * Threshold to decide if a vertex needs to be filtered.
     */
    private final Float threshold;
    /**
     * Random instance
     */
    private final Random randomGenerator;

    /**
     * Creates a new filter instance.
     *
     * @param sampleSize relative sample size
     * @param randomSeed random seed (can be {@code} null)
     */
    public VertexRandomFilter(Float sampleSize, Long randomSeed) {
      threshold = sampleSize;
      randomGenerator =
        (randomSeed != null) ? new Random(randomSeed) : new Random();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(VD vertex) throws Exception {
      return randomGenerator.nextFloat() < threshold;
    }
  }
}
