
package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Aggregate function returning the sum of a specified property over all
 * vertices.
 */
public class SumVertexProperty extends SumProperty implements VertexAggregateFunction {

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public SumVertexProperty(String propertyKey) {
    super(propertyKey);
  }

  @Override
  public PropertyValue getVertexIncrement(Vertex vertex) {
    return vertex.getPropertyValue(propertyKey);
  }
}
