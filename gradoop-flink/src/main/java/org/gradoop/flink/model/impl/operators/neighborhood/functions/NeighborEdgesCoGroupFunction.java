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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

public class NeighborEdgesCoGroupFunction implements CoGroupFunction<Vertex, Tuple2<GradoopId, Edge>, Vertex> {

  private EdgeAggregateFunction function;

  public NeighborEdgesCoGroupFunction(EdgeAggregateFunction function) {
    this.function = function;
  }

  @Override
  public void coGroup(Iterable<Vertex> vertices, Iterable<Tuple2<GradoopId, Edge>> tuples,
    Collector<Vertex> collector) throws Exception {
    Vertex vertex = vertices.iterator().next();
    PropertyValue propertyValue = PropertyValue.NULL_VALUE;
    boolean isFirst = true;

    for (Tuple2<GradoopId, Edge> tuple : tuples) {
      Edge edge = tuple.f1;
      if (isFirst) {
        isFirst = false;
        propertyValue = function.getEdgeIncrement(edge);
      } else {
        propertyValue = function.aggregate(propertyValue, function.getEdgeIncrement(edge));
      }
    }
    vertex.setProperty(function.getAggregatePropertyKey(), propertyValue);
    collector.collect(vertex);
  }
}
