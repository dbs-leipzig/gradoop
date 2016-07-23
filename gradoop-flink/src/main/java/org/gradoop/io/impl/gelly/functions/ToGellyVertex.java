package org.gradoop.io.impl.gelly.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

public class ToGellyVertex<V extends EPGMVertex>
  implements MapFunction<V, Vertex<GradoopId, V>> {
  @Override
  public Vertex<GradoopId, V> map(V v) throws Exception {
    return new Vertex<>(v.getId(), v);
  }
}
