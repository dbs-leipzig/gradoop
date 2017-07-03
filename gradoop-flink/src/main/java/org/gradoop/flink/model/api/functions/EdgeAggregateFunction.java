
package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;

/**
 * Describes an edge aggregate function as input for the
 * {@link Aggregation} operator.
 */
public interface EdgeAggregateFunction extends AggregateFunction {

  /**
   * Describes the increment of an edge that should be added to the aggregate.
   *
   * @param edge edge
   *
   * @return increment, may be NULL, which is handled in the operator
   */
  PropertyValue getEdgeIncrement(Edge edge);
}
