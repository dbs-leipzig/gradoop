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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.tuples.UndovetailingOPlusVertex;

/**
 * Projects the Tuple3 UndovetailingOPlusVertex either to <f0,f2> or to <f1,f2>.
 * f0 and f1 are transformed, too
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
@FunctionAnnotation.ForwardedFields("f2->f1")
public class MapFunctionProjectUndovetailingToGraphOperand implements MapFunction<UndovetailingOPlusVertex, Tuple2<GradoopId, Vertex>> {

  /**
   * Defines for which opreand is the UDF used for
   */
  private final boolean isLeft;

  /**
   * Default initialization
   * @param isLeft  If true, it corresponds to the left operand. Otherwise, it corresponds to the
   *              right one
   */
  public MapFunctionProjectUndovetailingToGraphOperand(boolean isLeft) {
    this.isLeft = isLeft;
  }

  @Override
  public Tuple2<GradoopId, Vertex> map(UndovetailingOPlusVertex x) throws Exception {
    return new Tuple2<GradoopId, Vertex>(isLeft ? x.f0.get() : x.f1.get(), x.f2);
  }
}
