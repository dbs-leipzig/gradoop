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
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

public class NeighborVertexReduceFunction implements
  GroupReduceFunction<Tuple2<Tuple2<Edge, Vertex>, Vertex>, Vertex> {

  private VertexAggregateFunction function;

  public NeighborVertexReduceFunction(VertexAggregateFunction function) {
    this.function = function;
  }

  @Override
  public void reduce(Iterable<Tuple2<Tuple2<Edge, Vertex>, Vertex>> tuples,
    Collector<Vertex> collector) throws Exception {

    PropertyValue propertyValue = PropertyValue.NULL_VALUE;
    Vertex vertex = null;
    Vertex edgeVertex;
    boolean isFirst = true;

    for (Tuple2<Tuple2<Edge, Vertex>, Vertex> tuple : tuples) {
      edgeVertex = tuple.f1;
      if (isFirst) {
        vertex = tuple.f0.f1;
        isFirst = false;
        propertyValue = function.getVertexIncrement(edgeVertex);
      } else {
        propertyValue = function.aggregate(propertyValue, function.getVertexIncrement(edgeVertex));
      }
    }
    vertex.setProperty(function.getAggregatePropertyKey(), propertyValue);
    collector.collect(vertex);
  }
}



