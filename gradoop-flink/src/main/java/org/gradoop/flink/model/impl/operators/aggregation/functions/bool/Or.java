
package org.gradoop.flink.model.impl.operators.aggregation.functions.bool;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Superclass of aggregate functions determining a predicate support.
 * e.g., graph contains a vertex labelled by "User"
 */
public abstract class Or implements AggregateFunction {

  @Override
  public PropertyValue aggregate(
    PropertyValue aggregate, PropertyValue increment) {
    return PropertyValueUtils.Boolean.or(aggregate, increment);
  }
}
