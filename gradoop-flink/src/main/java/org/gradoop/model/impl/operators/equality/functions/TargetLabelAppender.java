package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

public class TargetLabelAppender
  extends VertexLabelAppender
  implements JoinFunction<EdgeDataLabel, DataLabel, DataLabel> {

  @Override
  public DataLabel join(
    EdgeDataLabel edgeLabel, DataLabel targetLabel
  ) throws Exception {

    return new DataLabel(edgeLabel.getGraphId(),
      edgeLabel.getSourceId(), label(edgeLabel, targetLabel)
    );
  }
}
