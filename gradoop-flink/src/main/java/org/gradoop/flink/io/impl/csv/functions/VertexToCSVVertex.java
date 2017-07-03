
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.tuples.CSVVertex;

/**
 * Converts an {@link Vertex} into a CSV representation.
 *
 * Forwarded fields:
 *
 * label
 */
@FunctionAnnotation.ForwardedFields("label->f1")
public class VertexToCSVVertex extends ElementToCSV<Vertex, CSVVertex> {
  /**
   * Reduce object instantiations.
   */
  private final CSVVertex csvVertex = new CSVVertex();

  @Override
  public CSVVertex map(Vertex vertex) throws Exception {
    csvVertex.setId(vertex.getId().toString());
    csvVertex.setLabel(vertex.getLabel());
    csvVertex.setProperties(getPropertyString(vertex));
    return csvVertex;
  }
}
