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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Join the new GradoopIds, representing the new graphs, with the vertices by
 * adding them to the vertices graph sets
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1")
public class JoinVertexIdWithGraphIds
  implements JoinFunction<Tuple2<GradoopId, PropertyValue>,
  Tuple2<PropertyValue, GradoopId>, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> join(
    Tuple2<GradoopId, PropertyValue> vertexSplitKey,
      Tuple2<PropertyValue, GradoopId> splitKeyGradoopId) {
    reuseTuple.setFields(vertexSplitKey.f0, splitKeyGradoopId.f1);
    return reuseTuple;
  }
}
