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
