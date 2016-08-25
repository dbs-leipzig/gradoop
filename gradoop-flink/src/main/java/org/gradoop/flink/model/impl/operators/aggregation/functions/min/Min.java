package org.gradoop.flink.model.impl.operators.aggregation.functions.min;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValues;
import org.gradoop.flink.model.api.functions.AggregateFunction;

public abstract class Min implements AggregateFunction {
  /**
   * Property key whose value should be aggregated.
   */
  protected final String propertyKey;

  public Min(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate,
    PropertyValue increment) {
    return PropertyValues.min(aggregate, increment);
  }

  @Override
  public String getAggregatePropertyKey() {
    return "min(" + propertyKey + ")";
  }
}
