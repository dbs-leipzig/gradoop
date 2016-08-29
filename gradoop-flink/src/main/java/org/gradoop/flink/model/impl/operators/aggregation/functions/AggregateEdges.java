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

/**
 * edge,.. => aggregateValue
 */
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function.
 */
public class AggregateEdges
  implements GroupCombineFunction<Edge, PropertyValue> {

  /**
   * Aggregate function.
   */
  private final EdgeAggregateFunction aggFunc;

  /**
   * Constructor.
   *
   * @param aggFunc aggregate function
   */
  public AggregateEdges(EdgeAggregateFunction aggFunc) {
    this.aggFunc = aggFunc;
  }

  @Override
  public void combine(
    Iterable<Edge> edges, Collector<PropertyValue> out) throws Exception {
    PropertyValue aggregate = null;

    for (Edge edge : edges) {
      PropertyValue increment = aggFunc.getEdgeIncrement(edge);
      if (increment != null) {
        if (aggregate == null) {
          aggregate = increment;
        } else {
          aggregate = aggFunc.aggregate(aggregate, increment);
        }
      }
    }

    if (aggregate != null) {
      out.collect(aggregate);
    }
  }
}
