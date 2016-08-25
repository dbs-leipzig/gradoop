package org.gradoop.flink.model.impl.operators.aggregation.functions.min;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValues;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Created by peet on 25.08.16.
 */
public abstract class Min implements AggregateFunction {
  @Override
  public PropertyValue aggregate(PropertyValue aggregate,
    PropertyValue increment) {
    return PropertyValues.Numeric.min(aggregate, increment);
  }
}
