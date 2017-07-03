
package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

/**
 * Aggregate function returning the edge count of a graph / graph collection.
 */
public class EdgeCount extends Count implements EdgeAggregateFunction {

  @Override
  public PropertyValue getEdgeIncrement(Edge edge) {
    return PropertyValue.create(1L);
  }

  @Override
  public String getAggregatePropertyKey() {
    return "edgeCount";
  }
}
