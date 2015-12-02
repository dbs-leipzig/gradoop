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
 * (graphId, vertexIds, edgeIDs) x (graphId, graphCount) =>
 * (vertexIds, edgeIds, graphCount)
 */
public class VertexIdsEdgeIdsCountTriple  implements JoinFunction
  <Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>, Tuple2<GradoopId, Long>,
    Tuple3<GradoopIdSet, GradoopIdSet, Long>> {

  @Override
  public Tuple3<GradoopIdSet, GradoopIdSet, Long> join(
    Tuple3<GradoopId, GradoopIdSet, GradoopIdSet> graphIdVertexIdsEdgeIds,
    Tuple2<GradoopId, Long> graphIdCount
  ) {
    return new Tuple3<>(
      graphIdVertexIdsEdgeIds.f1,
      graphIdVertexIdsEdgeIds.f2,
      graphIdCount.f1
    );
  }
}
