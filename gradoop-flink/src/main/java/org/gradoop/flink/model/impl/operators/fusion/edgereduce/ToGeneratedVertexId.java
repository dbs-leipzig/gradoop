package org.gradoop.flink.model.impl.operators.fusion.edgereduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.tuples.VertexIdToEdgeId;

/**
 * Created by vasistas on 23/02/17.
 */
@FunctionAnnotation.ForwardedFields("f1->f1")
public class ToGeneratedVertexId implements MapFunction<VertexIdToEdgeId, VertexIdToEdgeId> {

  @Override
  public VertexIdToEdgeId map(VertexIdToEdgeId value) throws Exception {
    value.f0 = GradoopId.get();
    return value;
  }
}
