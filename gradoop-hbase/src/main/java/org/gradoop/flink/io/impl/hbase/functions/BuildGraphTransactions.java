
package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

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
  Tuple3<GradoopId, GradoopIdList, GradoopIdList>> {
  /**
   * Reduce object instantiations.
   */
  private final Tuple3<GradoopId, GradoopIdList, GradoopIdList> reuseTuple =
    new Tuple3<>();

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, GradoopId>> graphToVertexIds,
    Iterable<Tuple2<GradoopId, GradoopId>> graphToEdgeIds,
    Collector<Tuple3<GradoopId, GradoopIdList, GradoopIdList>> collector) throws
    Exception {

    GradoopIdList vertexIds  = new GradoopIdList();
    GradoopIdList edgeIds    = new GradoopIdList();
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
