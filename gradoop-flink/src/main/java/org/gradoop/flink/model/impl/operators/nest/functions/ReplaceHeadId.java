package org.gradoop.flink.model.impl.operators.nest.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Replaces the head id
 */
public class ReplaceHeadId implements
  JoinFunction<GraphHead, Tuple2<GradoopId, GradoopId>, GraphHead>,
  CrossFunction<GraphHead, Tuple2<GradoopId, GradoopId>, GraphHead> {

  @Override
  public GraphHead join(GraphHead graphHead,
    Tuple2<GradoopId, GradoopId> gradoopIdGradoopIdTuple2) throws Exception {
    graphHead.setId(gradoopIdGradoopIdTuple2.f0);
    return graphHead;
  }

  @Override
  public GraphHead cross(GraphHead graphHead,
    Tuple2<GradoopId, GradoopId> gradoopIdGradoopIdTuple2) throws Exception {
    graphHead.setId(gradoopIdGradoopIdTuple2.f0);
    return graphHead;
  }
}
