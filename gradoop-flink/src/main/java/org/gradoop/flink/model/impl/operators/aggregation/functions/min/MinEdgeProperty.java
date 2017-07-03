
package org.gradoop.flink.model.impl.operators.aggregation.functions.min;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function returning the minimum of a specified property over all
 * vertices.
 */
public class MinEdgeProperty extends MinProperty implements EdgeAggregateFunction {

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public MinEdgeProperty(String propertyKey) {
    super(propertyKey);
  }

  @Override
  public PropertyValue getEdgeIncrement(Edge edge) {
    return edge.getPropertyValue(propertyKey);
  }
}
