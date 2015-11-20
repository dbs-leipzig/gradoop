package org.gradoop.model.impl.operators.collection.binary.equality.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0,f1->f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
public class GraphIdVertexIdsEdgeIdsTriple implements
  JoinFunction<Tuple2<GradoopId,GradoopIds>,Tuple2<GradoopId,GradoopIds>,
    Tuple3<GradoopId,GradoopIds,GradoopIds>> {

  @Override
  public Tuple3<GradoopId, GradoopIds, GradoopIds> join(
    Tuple2<GradoopId, GradoopIds> graphIdVertexIds,
    Tuple2<GradoopId, GradoopIds> graphIdEdgeIds) {

    return new Tuple3<>(
      graphIdVertexIds.f0,
      graphIdVertexIds.f1,
      graphIdEdgeIds.f1
    );
  }
}
