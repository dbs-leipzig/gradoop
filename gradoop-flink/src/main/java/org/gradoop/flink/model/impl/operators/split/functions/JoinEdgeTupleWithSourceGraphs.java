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

package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Join edge tuples with the graph sets of their sources
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->f0")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class JoinEdgeTupleWithSourceGraphs<E extends Edge>
  implements JoinFunction<E, Tuple2<GradoopId, GradoopIdSet>,
  Tuple2<E, GradoopIdSet>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<E, GradoopIdSet> reuseTuple = new Tuple2<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<E, GradoopIdSet> join(
    E left, Tuple2<GradoopId, GradoopIdSet> right) {
    reuseTuple.f0 = left;
    reuseTuple.f1 = right.f1;
    return reuseTuple;
  }
}
