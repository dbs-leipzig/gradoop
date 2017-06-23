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

package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Sets the aggregation result as property for each vertex. All edges together with the
 * relevant vertex were grouped.
 */
public class NeighborEdgeReduceFunction
  extends NeighborEdgeFunction
  implements GroupReduceFunction<Tuple2<Edge, Vertex>, Vertex> {

  /**
   * Valued constructor.
   *
   * @param function edge aggregation function
   */
  public NeighborEdgeReduceFunction(EdgeAggregateFunction function) {
    super(function);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reduce(Iterable<Tuple2<Edge, Vertex>> tuples, Collector<Vertex> collector) throws
    Exception {

    PropertyValue propertyValue = PropertyValue.NULL_VALUE;
    Vertex vertex = null;
    Edge edge;
    boolean isFirst = true;

    for (Tuple2<Edge, Vertex> tuple: tuples) {
      edge = tuple.f0;
      if (isFirst) {
        //each tuple contains the same vertex
        vertex = tuple.f1;
        isFirst = false;
        propertyValue = getFunction().getEdgeIncrement(edge);
      } else {
        propertyValue = getFunction()
          .aggregate(propertyValue, getFunction().getEdgeIncrement(edge));
      }
    }
    vertex.setProperty(getFunction().getAggregatePropertyKey(), propertyValue);
    collector.collect(vertex);
  }
}



