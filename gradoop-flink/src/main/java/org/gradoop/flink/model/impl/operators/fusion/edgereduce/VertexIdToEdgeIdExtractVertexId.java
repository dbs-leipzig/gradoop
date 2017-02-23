package org.gradoop.flink.model.impl.operators.fusion.edgereduce;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.tuples.VertexIdToEdgeId;

/**
 * Created by vasistas on 23/02/17.
 */
@FunctionAnnotation.ForwardedFields("f0->*")
public class VertexIdToEdgeIdExtractVertexId implements
  org.apache.flink.api.java.functions.KeySelector<org.gradoop.flink.model.impl.operators.fusion
    .edgereduce.tuples.VertexIdToEdgeId, GradoopId> {
  @Override
  public GradoopId getKey(VertexIdToEdgeId value) throws Exception {
    return value.f0;
  }
}
