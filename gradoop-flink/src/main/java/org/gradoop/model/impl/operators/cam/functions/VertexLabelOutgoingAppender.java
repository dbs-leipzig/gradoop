package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

public class VertexLabelOutgoingAppender implements JoinFunction<VertexLabel,
  EdgeLabel, VertexLabel>{

  @Override
  public VertexLabel join(VertexLabel vertexLabel, EdgeLabel edgeLabel) throws
    Exception {
    return null;
  }
}
