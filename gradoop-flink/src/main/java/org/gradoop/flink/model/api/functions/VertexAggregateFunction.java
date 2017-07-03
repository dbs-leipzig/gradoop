
package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

/**
 * Describes a vertex aggregate function as input for the
 * {@link Aggregation} operator.
 */
public interface VertexAggregateFunction extends AggregateFunction {

  /**
   * Describes the increment of a vertex that should be added to the aggregate.
   *
   * @param vertex vertex
   *
   * @return increment, may be NULL, which is handled in the operator
   */
  PropertyValue getVertexIncrement(Vertex vertex);
}
