package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

/**
 * "edgeLabel","sourceLabel" => "edgeLabel[sourceLabel]"
 */
public class SourceLabelAppender
  extends VertexLabelAppender
  implements JoinFunction<EdgeDataLabel, DataLabel, DataLabel> {

  @Override
  public DataLabel join(
    EdgeDataLabel edgeLabel, DataLabel sourceLabel
  ) throws Exception {

    return new DataLabel(
      edgeLabel.getTargetId(), label(sourceLabel, edgeLabel)
    );
  }
}
