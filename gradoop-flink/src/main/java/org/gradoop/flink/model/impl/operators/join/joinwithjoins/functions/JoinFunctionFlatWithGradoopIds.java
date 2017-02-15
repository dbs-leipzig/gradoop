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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.UndovetailingOPlusVertex;

import java.io.Serializable;

/**
 * Implements the graph join for both the inner, left right and full for the edge semantics
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class JoinFunctionFlatWithGradoopIds implements
  FlatJoinFunction<Tuple2<Vertex, OptSerializableGradoopId>,
    Tuple2<Vertex, OptSerializableGradoopId>, UndovetailingOPlusVertex>, Serializable {

  /*
   * Function used to check if the vertices match or not
   */
  private final Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertexF;

  /**
   * Function used to combine the vertices together
   */
  private final OplusVertex combineVertices;

  /**
   * Provides the implementation of the join function between the vertices. The result is the merged
   * vertex with the ids of the corresponding elements (vertices) coming from either the left or
   * the right operand or both.
   *
   * @param thetaVertexF        Function used to check if the vertices match or not
   * @param combineVertices     Function used to combine the vertices together
   */
  public JoinFunctionFlatWithGradoopIds(Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertexF,
    OplusVertex combineVertices) {
    this.thetaVertexF = thetaVertexF;
    this.combineVertices = combineVertices;
  }

  @Override
  public void join(Tuple2<Vertex, OptSerializableGradoopId> first,
    Tuple2<Vertex, OptSerializableGradoopId> second,
    Collector<UndovetailingOPlusVertex> out) throws Exception {
    // Inner join condition
    if (first != null && second != null && first.f1.isPresent() && second.f1.isPresent()) {
      Vertex ff0 = first.getField(0), sf0 = second.getField(0);
      if (ff0 == null)
        throw new RuntimeException("ff0 is null");
      else if (sf0 == null)
        throw new RuntimeException("sf0 is null");
      if (thetaVertexF.apply(new Tuple2<>(ff0, sf0))) {
        out.collect(new UndovetailingOPlusVertex(OptSerializableGradoopId.value(ff0.getId()),
          OptSerializableGradoopId.value(sf0.getId()),
          combineVertices.apply(new Tuple2<>(ff0, sf0))));
      }
    } else {
      // outer join conditions:
      // * left or/and
      // * full
      if (first == null || !first.f1.isPresent()) {
        Vertex sf0 = second.getField(0);
        out.collect(
          new UndovetailingOPlusVertex(OptSerializableGradoopId.empty(),
            OptSerializableGradoopId.value(sf0.getId()), sf0));
      }
      // outer join conditions:
      // * right or/and
      // * full
      if (second == null || !second.f1.isPresent()) {
        Vertex ff0 = first.getField(0);
        out.collect(
          new UndovetailingOPlusVertex(OptSerializableGradoopId.value(ff0.getId()),
            OptSerializableGradoopId.empty(), ff0));
      }
    }
  }
}
