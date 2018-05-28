/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
