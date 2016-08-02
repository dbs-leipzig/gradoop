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

package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.UnaryFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Maps the vertices to pairs, where each pair contains the vertex id and one
 * split value. The split values are determined using a user defined function.
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("properties")
public class SplitValues<V extends Vertex>
  implements FlatMapFunction<V, Tuple2<GradoopId, PropertyValue>> {
  /**
   * Self defined Function
   */
  private UnaryFunction<V, List<PropertyValue>> function;

  /**
   * Constructor
   *
   * @param function user-defined function to determine split values
   */
  public SplitValues(UnaryFunction<V, List<PropertyValue>> function) {
    this.function = checkNotNull(function);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(V vertex,
    Collector<Tuple2<GradoopId, PropertyValue>> collector) throws Exception {
    List<PropertyValue> splitValues = function.execute(vertex);
    for (PropertyValue value : splitValues) {
      collector.collect(new Tuple2<>(vertex.getId(), value));
    }
  }
}

