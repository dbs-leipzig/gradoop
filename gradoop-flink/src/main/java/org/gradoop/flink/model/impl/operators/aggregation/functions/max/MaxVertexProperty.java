
package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Aggregate function returning the minimum of a specified property over all
 * vertices.
 */
public class MaxVertexProperty extends MaxProperty implements VertexAggregateFunction {

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public MaxVertexProperty(String propertyKey) {
    super(propertyKey);
  }

  @Override
  public PropertyValue getVertexIncrement(Vertex vertex) {
    return vertex.getPropertyValue(propertyKey);
  }
}
