package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValues;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Created by peet on 25.08.16.
 */
public abstract class Sum implements AggregateFunction {
  public PropertyValue aggregate(
    PropertyValue aggregate, PropertyValue increment) {
    return PropertyValues.Numeric.add(aggregate, increment);
  }
}
