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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Changes the source of the edge with a nested vertex
 */
@FunctionAnnotation.ForwardedFieldsFirst(
  "targetId -> targetId; label -> label; graphIds -> " + "graphIds; properties -> properties")
@FunctionAnnotation.ForwardedFieldsSecond("f1 -> sourceId")
public class UpdateEdgesOnSource implements
  JoinFunction<Edge, Tuple3<GradoopId, GradoopId, GradoopId>, Edge> {
  @Override
  public Edge join(Edge edge, Tuple3<GradoopId, GradoopId, GradoopId> patternMatched) throws
    Exception {
    if (patternMatched != null) {
      edge.setId(GradoopId.get());
      GradoopId id = patternMatched.f1;
      if (id.equals(GradoopId.NULL_VALUE)) {
        id = patternMatched.f2;
      }
      edge.setSourceId(id);
    }
    return edge;
  }
}
