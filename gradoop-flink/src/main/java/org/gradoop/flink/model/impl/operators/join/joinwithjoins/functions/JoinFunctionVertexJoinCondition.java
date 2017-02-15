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

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.UndovetailingOPlusVertex;

import java.io.Serializable;

/**
 * Implements the join operation for the vertices
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class JoinFunctionVertexJoinCondition extends
  RichFlatJoinFunction<Vertex, Vertex, UndovetailingOPlusVertex> implements Serializable {
  private final Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertexF;
  private final OplusVertex combineVertices;

  public JoinFunctionVertexJoinCondition(Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertexF,
    OplusVertex combineVertices) {
    this.thetaVertexF = thetaVertexF;
    this.combineVertices = combineVertices;
  }

  @Override
  public void join(Vertex first, Vertex second, Collector<UndovetailingOPlusVertex> out) throws
    Exception {
    if (first != null && second != null) {
      if (thetaVertexF.apply(new Tuple2<>(first, second))) {
        out.collect(new UndovetailingOPlusVertex(OptSerializableGradoopId.value(first.getId()),
          OptSerializableGradoopId.value(second.getId()),
          combineVertices.apply(new Tuple2<>(first, second))));
      }
    } else if (first == null) {
      out.collect(
        new UndovetailingOPlusVertex(OptSerializableGradoopId.empty(), OptSerializableGradoopId.value(second.getId()),
          second));
    } else {
      out.collect(
        new UndovetailingOPlusVertex(OptSerializableGradoopId.value(first.getId()), OptSerializableGradoopId.empty(),
          first));
    }
  }
}
