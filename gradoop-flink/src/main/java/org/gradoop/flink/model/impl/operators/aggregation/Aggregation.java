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

package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.CombinePartitionAggregates;
import org.gradoop.flink.model.impl.operators.aggregation.functions.SetAggregateProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateEdges;
import org.gradoop.flink.model.impl.operators.aggregation.functions.AggregateVertices;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 */
public class Aggregation implements UnaryGraphToGraphOperator {

  /**
   * User-defined aggregate function which is applied on a single logical graph.
   */
  private final AggregateFunction aggregateFunction;

  /**
   * Creates new aggregation.
   *
   * @param aggregateFunction  user defined aggregation function which gets
   *                             called on the input graph
   */
  public Aggregation(final AggregateFunction aggregateFunction) {
    this.aggregateFunction = checkNotNull(aggregateFunction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    DataSet<Vertex> vertices = graph.getVertices();
    DataSet<Edge> edges = graph.getEdges();

    DataSet<PropertyValue> aggregate;

    if (this.aggregateFunction instanceof VertexAggregateFunction) {
      aggregate = aggregateVertices(vertices);

    } else {
      aggregate = aggregateEdges(edges);
    }

    DataSet<PropertyValue> nullValue = graph
      .getConfig()
      .getExecutionEnvironment()
      .fromElements(PropertyValue.NULL_VALUE);

    aggregate = aggregate
      .reduceGroup(new CombinePartitionAggregates(aggregateFunction))
      .union(nullValue)
      .reduceGroup(new SetNullIfEmpty());

    DataSet<GraphHead> graphHead = graph.getGraphHead()
      .map(new SetAggregateProperty(aggregateFunction))
      .withBroadcastSet(aggregate, SetAggregateProperty.VALUE);

    return LogicalGraph
      .fromDataSets(graphHead, vertices, edges, graph.getConfig());
  }

  /**
   * Applies an aggregate function to the partitions of a vertex data set.
   *
   * @param vertices vertex data set
   * @return partition aggregate value
   */
  private DataSet<PropertyValue> aggregateVertices(DataSet<Vertex> vertices) {
    return vertices
      .combineGroup(new AggregateVertices(
        (VertexAggregateFunction) aggregateFunction));
  }

  /**
   * Applies an aggregate function to the partitions of an edge data set.
   *
   * @param edges edge data set
   * @return partition aggregate value
   */
  private DataSet<PropertyValue> aggregateEdges(DataSet<Edge> edges) {
    return edges
      .combineGroup(new AggregateEdges(
        (EdgeAggregateFunction) aggregateFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Aggregation.class.getName();
  }
}
