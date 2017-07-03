
package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Superclass of aggregate functions that determine a maximal value.
 */
public abstract class Max implements AggregateFunction {

  @Override
  public PropertyValue aggregate(
    PropertyValue aggregate, PropertyValue increment) {
    return PropertyValueUtils.Numeric.max(aggregate, increment);
  }
}
