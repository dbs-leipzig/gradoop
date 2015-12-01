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
