package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValues;
import org.gradoop.flink.model.api.functions.AggregateFunction;

public abstract class MaxProperty extends Max {
  /**
   * Property key whose value should be aggregated.
   */
  protected final String propertyKey;

  public MaxProperty(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return "max(" + propertyKey + ")";
  }
}
