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
 * (graphId,vertex),.. => (graphId,aggregateValue),..
 */
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

import java.util.Iterator;

/**
 * Aggregate function.
 */
public class ApplyAggregateVertices implements GroupCombineFunction
  <Tuple2<GradoopId, Vertex>, Tuple2<GradoopId, PropertyValue>> {

  /**
   * Aggregate function.
   */
  private final VertexAggregateFunction aggFunc;
  /**
   * Reuse tuple.
   */
  private final Tuple2<GradoopId, PropertyValue> reusePair = new Tuple2<>();

  /**
   * Constructor.
   *
   * @param aggFunc aggregate function
   */
  public ApplyAggregateVertices(VertexAggregateFunction aggFunc) {
    this.aggFunc = aggFunc;
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, Vertex>> vertices,
    Collector<Tuple2<GradoopId, PropertyValue>> out) throws Exception {

    Iterator<Tuple2<GradoopId, Vertex>> iterator = vertices.iterator();

    Tuple2<GradoopId, Vertex> graphIdVertex = iterator.next();
    Vertex vertex = graphIdVertex.f1;
    PropertyValue aggregate = aggFunc.getVertexIncrement(vertex);

    while (iterator.hasNext()) {
      vertex = iterator.next().f1;
      PropertyValue increment = aggFunc.getVertexIncrement(vertex);

      if (increment != null) {
        if (aggregate == null) {
          aggregate = increment;
        } else {
          aggregate = aggFunc.aggregate(aggregate, increment);
        }
      }
    }

    if (aggregate != null) {
      reusePair.f0 = graphIdVertex.f0;
      reusePair.f1 = aggregate;
      out.collect(reusePair);
    }
  }
}
