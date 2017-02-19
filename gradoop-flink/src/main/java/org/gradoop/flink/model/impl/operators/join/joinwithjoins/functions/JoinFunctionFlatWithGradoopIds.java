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
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.DisambiguationTupleWithVertexId;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.UndovetailingOPlusVertex;

import java.io.Serializable;

/**
 * Implements the graph join for both the inner, left right and full for the edge semantics
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0;f2->f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2;f2->f3")
public class JoinFunctionFlatWithGradoopIds implements
  FlatJoinFunction<DisambiguationTupleWithVertexId, DisambiguationTupleWithVertexId, UndovetailingOPlusVertex>, Serializable {

  /**
   * Tuple reusable for internal computations
   */
  private final Tuple2<Vertex, Vertex> reusableTuple;

  /**
   * Function used to check if the vertices match or not
   */
  private final Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertexF;

  /**
   * Function used to combine the vertices together
   */
  private final OplusVertex combineVertices;

  /**
   * Yet another reusable field for providing a result
   */
  private final UndovetailingOPlusVertex reusableU;

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
    reusableTuple = new Tuple2<>();
    reusableU = new UndovetailingOPlusVertex();
  }

  @Override
  public void join(DisambiguationTupleWithVertexId first,
    DisambiguationTupleWithVertexId second,
    Collector<UndovetailingOPlusVertex> out) throws Exception {
    // Inner join condition
    if (first != null && second != null && first.f1 && second.f1) {
      Vertex ff0 = first.getField(0);
      Vertex sf0 = second.getField(0);
      reusableTuple.f0 = ff0;
      reusableTuple.f1 = sf0;
      if (thetaVertexF.apply(reusableTuple)) {
        reusableU.f0 = true;
        reusableU.f1 = ff0.getId();
        reusableU.f2 = true;
        reusableU.f3 = sf0.getId();
        reusableU.f4 = combineVertices.apply(reusableTuple);
        out.collect(reusableU);
      }
    } else {
      // outer join conditions:
      // * left or/and
      // * full
      if (first == null || !first.f1) {
        Vertex sf0 = second.getField(0);
        reusableU.f0 = false;
        reusableU.f1 = GradoopId.NULL_VALUE;
        reusableU.f2 = true;
        reusableU.f3 = sf0.getId();
        reusableU.f4 = sf0;
        out.collect(reusableU);
      }
      // outer join conditions:
      // * right or/and
      // * full
      if (second == null || !second.f1) {
        Vertex ff0 = first.getField(0);
        reusableU.f0 = true;
        reusableU.f1 = ff0.getId();
        reusableU.f2 = false;
        reusableU.f3 = GradoopId.NULL_VALUE;
        reusableU.f4 = ff0;
        out.collect(reusableU);
      }
    }
  }
}
