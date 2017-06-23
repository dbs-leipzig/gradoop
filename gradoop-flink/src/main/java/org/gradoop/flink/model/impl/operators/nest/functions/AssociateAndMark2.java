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

package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Uses the join for two different semantic purposes, bot returning the new vertices and
 * updating the old edges.
 *
 * Warning: it assumes that the search graph element is always present (leftJoin) and
 * that there could not be a right element match.
 *
 * f0 = search graph id
 * f1 = graph patterns id
 * f2 = vertex id
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0 -> f0; f1 -> f1;")
@FunctionAnnotation.ForwardedFieldsSecond("f0 -> f2")
public class AssociateAndMark2 implements
  JoinFunction<Tuple2<GradoopId, GradoopId>,
               Tuple2<GradoopId, GradoopId>, Tuple3<GradoopId, GradoopId, GradoopId>
              > {

  /**
   * Reusable element
   */
  private final Tuple3<GradoopId, GradoopId, GradoopId> q;

  /**
   * Default constructor
   */
  public AssociateAndMark2() {
    q = new Tuple3<>();
  }

  @Override
  public Tuple3<GradoopId, GradoopId, GradoopId> join(Tuple2<GradoopId, GradoopId> graphIndexEntry,
                       Tuple2<GradoopId, GradoopId> collectionIndexEntry) throws Exception {
    q.f0 = graphIndexEntry.f0;
    q.f1 = collectionIndexEntry != null ? collectionIndexEntry.f0 : GradoopId.NULL_VALUE;
    q.f2 = graphIndexEntry.f1;
    return q;
  }
}
