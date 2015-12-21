package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

public class SourceLabelUpdater implements JoinFunction<EdgeLabel,
  VertexLabel, EdgeLabel> {
  @Override
  public EdgeLabel join(
    EdgeLabel edgeLabel, VertexLabel sourceLabel) throws Exception {

    edgeLabel.setSourceLabel(sourceLabel.getLabel());

    return edgeLabel;
  }
}
