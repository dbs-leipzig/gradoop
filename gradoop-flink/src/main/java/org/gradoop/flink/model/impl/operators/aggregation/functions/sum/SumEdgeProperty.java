
package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function returning the sum of a specified property over all edges.
 */
public class SumEdgeProperty extends SumProperty
  implements EdgeAggregateFunction {

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public SumEdgeProperty(String propertyKey) {
    super(propertyKey);
  }

  @Override
  public PropertyValue getEdgeIncrement(Edge edge) {
    return edge.getPropertyValue(propertyKey);
  }
}
