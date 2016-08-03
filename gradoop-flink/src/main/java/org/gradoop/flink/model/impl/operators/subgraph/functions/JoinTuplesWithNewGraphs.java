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
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Join a tuple of an element id and a graph id with a dictionary mapping each
 * graph to a new graph. Result is a tuple of the id of the element and the id
 * of the new graph.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class JoinTuplesWithNewGraphs
  implements JoinFunction<
  Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>,
  Tuple2<GradoopId, GradoopId>> {


  /**
   * Reduce object instantiations
   */
  private Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> join(
    Tuple2<GradoopId, GradoopId> left,
    Tuple2<GradoopId, GradoopId> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = right.f1;
    return reuseTuple;
  }
}
