
package org.gradoop.flink.model.impl.operators.aggregation.functions.containment;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * aggregate and filter function to presence of a vertex label in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public class HasVertexLabel
  extends HasLabel implements VertexAggregateFunction {

  /**
   * Constructor.
   *
   * @param label vertex label to check presence of
   */
  public HasVertexLabel(String label) {
    super(label);
  }

  @Override
  public PropertyValue getVertexIncrement(Vertex vertex) {
    return PropertyValue.create(vertex.getLabel().equals(label));
  }

  @Override
  public String getAggregatePropertyKey() {
    return "hasVertexLabel_" + label;
  }

}
