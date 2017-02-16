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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;

/**
 * Given a vertex and an edge, coming that
 *
 * Created by Giacomo Bergami on 15/02/17.
 */
public class JoinFunctionWithVertexAndGradoopIdFromEdge implements
  JoinFunction<Vertex, Edge, Tuple3<Vertex, Boolean, GradoopId>> {
  @Override
  public Tuple3<Vertex, Boolean, GradoopId> join(Vertex first, Edge second) throws
    Exception {
    return new Tuple3<>(
      first,
      second != null,
      second == null ? GradoopId.NULL_VALUE : second.getId()
    );
  }
}
