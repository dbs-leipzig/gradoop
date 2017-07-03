
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.csv.tuples.CSVEdge;

/**
 * Converts an {@link Edge} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f3")
public class EdgeToCSVEdge extends ElementToCSV<Edge, CSVEdge> {
  /**
   * Reduce object instantiations
   */
  private final CSVEdge csvEdge = new CSVEdge();

  @Override
  public CSVEdge map(Edge edge) throws Exception {
    csvEdge.setId(edge.getId().toString());
    csvEdge.setSourceId(edge.getSourceId().toString());
    csvEdge.setTargetId(edge.getTargetId().toString());
    csvEdge.setLabel(edge.getLabel());
    csvEdge.setProperties(getPropertyString(edge));
    return csvEdge;
  }
}
