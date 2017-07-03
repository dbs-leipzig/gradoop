
package org.gradoop.flink.model.impl.operators.aggregation.functions.min;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Superclass of aggregate functions that determine a minimal value.
 */
public abstract class Min implements AggregateFunction {

  @Override
  public PropertyValue aggregate(PropertyValue aggregate,
    PropertyValue increment) {
    return PropertyValueUtils.Numeric.min(aggregate, increment);
  }
}
