package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

/**
 * Created by peet on 03.02.16.
 */
public class TargetLabelUpdater implements
  JoinFunction<EdgeLabel, VertexLabel, EdgeLabel> {

  @Override
  public EdgeLabel join(
    EdgeLabel edgeLabel, VertexLabel targetLabel) throws Exception {

    edgeLabel.setTargetLabel(targetLabel.getLabel());

    return edgeLabel;
  }
}
