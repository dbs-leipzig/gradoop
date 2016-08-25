package org.gradoop.flink.model.impl.operators.aggregation.functions.or;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValues;
import org.gradoop.flink.model.api.functions.AggregateFunction;

public abstract class Or implements AggregateFunction {

  @Override
  public PropertyValue aggregate(
    PropertyValue aggregate, PropertyValue increment) {
    return PropertyValues.Boolean.or(aggregate, increment);
  }
}
