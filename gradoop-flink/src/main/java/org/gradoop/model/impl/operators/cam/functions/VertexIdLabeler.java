package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

public class VertexIdLabeler<V extends EPGMVertex>
  extends EPGMElementDataLabeler<V>
  implements VertexLabeler<V> {

  @Override
  public void flatMap(V vertex, Collector<VertexLabel> collector) throws Exception {

    GradoopId vertexId = vertex.getId();
    String vertexLabel = "(" + vertex.getId() + ")";

    for(GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(new VertexLabel(graphId, vertexId, vertexLabel));
    }

  }
}
