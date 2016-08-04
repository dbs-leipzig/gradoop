package org.gradoop.flink.io.impl.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

public class ToGellyVertex
  implements MapFunction<Vertex,
  org.apache.flink.graph.Vertex<GradoopId, Vertex>> {
  @Override
  public org.apache.flink.graph.Vertex<GradoopId, Vertex> map(Vertex v)
    throws Exception {
    return new org.apache.flink.graph.Vertex<>(v.getId(), v);
  }
}
