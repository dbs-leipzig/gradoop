package org.gradoop.model.impl.operators.equality.functions;

import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

public abstract class VertexLabelAppender {

  protected String label(EdgeDataLabel edgeLabel, DataLabel vertexLabel) {
    return vertexLabel.getLabel() + "[" + edgeLabel.getLabel() + "]";
  }
}
