package org.gradoop.model.impl.operators.equality.functions;

import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

/**
 * "edgeLabel", "vertexLAbel" => "vertexLabel[edgeLabel]"
 */
public abstract class VertexLabelAppender {

  /**
   * Appends the label.
   *
   * @param vertexLabel vertex label
   * @param edgeLabel edge label
   * @return appended label
   */
  protected String label(DataLabel vertexLabel, EdgeDataLabel edgeLabel) {
    return vertexLabel.getLabel() + "[" + edgeLabel.getLabel() + "]";
  }
}
