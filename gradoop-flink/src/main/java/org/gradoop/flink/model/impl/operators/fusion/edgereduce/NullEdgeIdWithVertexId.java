package org.gradoop.flink.model.impl.operators.fusion.edgereduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.tuples.VertexIdToEdgeId;

/**
 * Created by vasistas on 23/02/17.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class NullEdgeIdWithVertexId implements MapFunction<Vertex, VertexIdToEdgeId> {

  private final VertexIdToEdgeId reusable;

  public NullEdgeIdWithVertexId() {
    reusable = new VertexIdToEdgeId();
    reusable.f1 = GradoopId.NULL_VALUE;
  }

  @Override
  public VertexIdToEdgeId map(Vertex value) throws Exception {
    reusable.f0 = value.getId();
    return reusable;
  }
}
