
package org.gradoop.flink.model.impl.operators.aggregation.functions.containment;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * aggregate and filter function to presence of an edge label in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public class HasEdgeLabel extends HasLabel implements EdgeAggregateFunction {

  /**
   * Constructor.
   *
   * @param label vertex label to check presence of
   */
  public HasEdgeLabel(String label) {
    super(label);
  }

  @Override
  public PropertyValue getEdgeIncrement(Edge edge) {
    return PropertyValue.create(edge.getLabel().equals(label));
  }

  @Override
  public String getAggregatePropertyKey() {
    return "hasEdgeLabel_" + label;
  }
}
