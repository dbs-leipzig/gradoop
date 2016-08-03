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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Join the edge tuples with the new graph ids.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f1->f1;f2->f2")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f3")
public class EdgesWithNewGraphsTuple
  implements JoinFunction<
  Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopId>,
  Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> reuseTuple =
    new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> join(
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> left,
    Tuple2<GradoopId, GradoopId> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = left.f1;
    reuseTuple.f2 = left.f2;
    reuseTuple.f3 = right.f1;
    return reuseTuple;
  }
}
