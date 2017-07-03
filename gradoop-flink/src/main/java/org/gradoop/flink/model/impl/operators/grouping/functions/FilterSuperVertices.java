
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

/**
 * Filter those tuples which are used to create new super vertices.
 */
@FunctionAnnotation.ReadFields("f5")
public class FilterSuperVertices implements FilterFunction<VertexGroupItem> {

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean filter(VertexGroupItem vertexGroupItem) throws Exception {
    return vertexGroupItem.isSuperVertex();
  }
}
