package org.gradoop.flink.io.impl.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

public class ToGellyEdge
  implements MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, Edge>> {
  @Override
  public org.apache.flink.graph.Edge<GradoopId, Edge> map(Edge e)
    throws Exception {
    return new org.apache.flink.graph.Edge<>(
      e.getSourceId(), e.getTargetId(), e);
  }
}
