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

package org.gradoop.model.impl.operators.split.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * Join edge tuples with the graph sets of their targets
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0;f1")
public class JoinEdgeTupleWithTargetGraphs<E extends EPGMEdge>
  implements JoinFunction
  <Tuple3<E, List<GradoopId>, GradoopId>, Tuple2<GradoopId, List<GradoopId>>,
    Tuple3<E, List<GradoopId>, List<GradoopId>>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<E, List<GradoopId>, List<GradoopId>> reuseTuple =
    new Tuple3<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple3<E, List<GradoopId>, List<GradoopId>> join(
    Tuple3<E, List<GradoopId>, GradoopId> left,
    Tuple2<GradoopId, List<GradoopId>> right) throws Exception {
    reuseTuple.f0 = left.f0;
    reuseTuple.f1 = left.f1;
    reuseTuple.f2 = Lists.newArrayList(right.f1);
    return reuseTuple;
  }
}
