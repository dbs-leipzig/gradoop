package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValues;
import org.gradoop.flink.model.api.functions.AggregateFunction;

public abstract class Sum implements AggregateFunction {
  /**
   * Property key whose value should be aggregated.
   */
  protected final String propertyKey;

  public Sum(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue aggregate(
    PropertyValue aggregate, PropertyValue increment) {
    return PropertyValues.add(aggregate, increment);
  }

  @Override
  public String getAggregatePropertyKey() {
    return "sum(" + propertyKey + ")";
  }
}
