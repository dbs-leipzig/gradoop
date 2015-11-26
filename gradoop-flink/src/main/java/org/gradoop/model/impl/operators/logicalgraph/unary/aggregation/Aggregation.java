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

package org.gradoop.model.impl.operators.logicalgraph.unary.aggregation;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.PropertySetter;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 * @param <N>  output type of aggregate function
 */
public class Aggregation<N extends Number,
  G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<V, E, G> {

  /**
   * Used to store aggregate result.
   */
  private final String aggregatePropertyKey;
  /**
   * User defined aggregate function.
   */
  private final AggregateFunction<N, G, V, E> aggregationFunc;

  /**
   * Creates new aggregation.
   *
   * @param aggregatePropertyKey property key to store result of {@code
   *                             aggregationFunc}
   * @param aggregationFunc      user defined aggregation function which gets
   *                             called on the input graph
   */
  public Aggregation(final String aggregatePropertyKey,
    AggregateFunction<N, G, V, E> aggregationFunc) {
    this.aggregatePropertyKey = aggregatePropertyKey;
    this.aggregationFunc = aggregationFunc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) throws
    Exception {

    DataSet<N> aggregateValue = aggregationFunc.execute(graph);

    DataSet<G> graphHead = graph.getGraphHead()
      .map(new PropertySetter<G>(aggregatePropertyKey))
      .withBroadcastSet(aggregateValue, PropertySetter.VALUE);

    return LogicalGraph.fromDataSets(
        graphHead,
        graph.getVertices(),
        graph.getEdges(),
        graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Aggregation.class.getName();
  }
}
