package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * represents a vertex by an id string
 */
public class VertexToIdString extends ElementToDataString<Vertex>
  implements VertexToString<Vertex> {

  @Override
  public void flatMap(Vertex vertex, Collector<VertexString> collector)
      throws Exception {

    GradoopId vertexId = vertex.getId();
    String vertexLabel = "(" + vertex.getId() + ")";

    for (GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(new VertexString(graphId, vertexId, vertexLabel));
    }

  }
}
