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

package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * (graphId, vertexIds) x (graphId, edgeIds) => (graphId, vertexIds, edgeIds)
 */
//@FunctionAnnotation.ForwardedFieldsFirst("f0,f1")
//@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class GraphIdVertexIdsEdgeIdsTriple implements
  JoinFunction<Tuple2<GradoopId, GradoopIdSet>, Tuple2<GradoopId, GradoopIdSet>,
    Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>> {

  @Override
  public Tuple3<GradoopId, GradoopIdSet, GradoopIdSet> join(
    Tuple2<GradoopId, GradoopIdSet> graphIdVertexIds,
    Tuple2<GradoopId, GradoopIdSet> graphIdEdgeIds) {

    return new Tuple3<>(
      graphIdVertexIds.f0,
      graphIdVertexIds.f1,
      graphIdEdgeIds.f1
    );
  }
}
