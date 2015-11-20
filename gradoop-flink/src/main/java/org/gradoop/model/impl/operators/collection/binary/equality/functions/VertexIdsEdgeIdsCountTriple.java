package org.gradoop.model.impl.operators.collection.binary.equality.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;

@FunctionAnnotation.ForwardedFieldsFirst("f1->f0,f2->f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class VertexIdsEdgeIdsCountTriple  implements JoinFunction
  <Tuple3<GradoopId, GradoopIds, GradoopIds>, Tuple2<GradoopId, Long>,
    Tuple3<GradoopIds, GradoopIds, Long>> {

  @Override
  public Tuple3<GradoopIds, GradoopIds, Long> join(
    Tuple3<GradoopId, GradoopIds, GradoopIds> graphIdVertexIdsEdgeIds,
    Tuple2<GradoopId, Long> graphIdCount
  ) {
    return new Tuple3<>(
      graphIdVertexIdsEdgeIds.f1,
      graphIdVertexIdsEdgeIds.f2,
      graphIdCount.f1
    );
  }
}
