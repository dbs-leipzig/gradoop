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
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Join an edge tuple with a tuple containing the target vertex id of this edge
 * and its new graphs.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f1->f1;f3->f3")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class JoinWithTargetGraphIdSet
  implements JoinFunction<
  Tuple4<GradoopId, GradoopIdSet, GradoopId, GradoopIdSet>,
  Tuple2<GradoopId, GradoopIdSet>,
  Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet>> {

  /**
   * Reduce object instantiations
   */
  private Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet> reuseTuple
    = new Tuple4<>();

  @Override
  public Tuple4<GradoopId, GradoopIdSet, GradoopIdSet, GradoopIdSet> join(
    Tuple4<GradoopId, GradoopIdSet, GradoopId, GradoopIdSet> edge,
    Tuple2<GradoopId, GradoopIdSet> vertex) throws
    Exception {
    reuseTuple.f0 = edge.f0;
    reuseTuple.f1 = edge.f1;
    reuseTuple.f2 = vertex.f1;
    reuseTuple.f3 = edge.f3;
    return reuseTuple;
  }
}
