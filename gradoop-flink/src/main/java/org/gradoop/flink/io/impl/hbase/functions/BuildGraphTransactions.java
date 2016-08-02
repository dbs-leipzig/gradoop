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

package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Co-groups graph-id, vertex-id) and (graph-id, edge-id) tuples to
 * (graph-id, {vertex-id}, {edge-id}) triples\
 *
 * Forwarded fields first:
 *
 * f0: graph id
 *
 * Read fields first:
 *
 * f1: vertex id
 *
 * Read fields second:
 *
 * f1: edge id
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class BuildGraphTransactions implements CoGroupFunction<
  Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopId>,
  Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>> {
  /**
   * Reduce object instantiations.
   */
  private final Tuple3<GradoopId, GradoopIdSet, GradoopIdSet> reuseTuple =
    new Tuple3<>();

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, GradoopId>> graphToVertexIds,
    Iterable<Tuple2<GradoopId, GradoopId>> graphToEdgeIds,
    Collector<Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>> collector) throws
    Exception {

    GradoopIdSet vertexIds  = new GradoopIdSet();
    GradoopIdSet edgeIds    = new GradoopIdSet();
    boolean initialized     = false;

    for (Tuple2<GradoopId, GradoopId> graphToVertexTuple : graphToVertexIds) {
      if (!initialized) {
        reuseTuple.f0 = graphToVertexTuple.f0;
        initialized   = true;
      }
      vertexIds.add(graphToVertexTuple.f1);
    }
    for (Tuple2<GradoopId, GradoopId> graphToEdgeTuple : graphToEdgeIds) {
      edgeIds.add(graphToEdgeTuple.f1);
    }
    reuseTuple.f1 = vertexIds;
    reuseTuple.f2 = edgeIds;
    collector.collect(reuseTuple);
  }
}
