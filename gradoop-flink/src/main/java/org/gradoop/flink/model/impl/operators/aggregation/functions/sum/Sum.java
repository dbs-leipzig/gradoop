
package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Superclass of summing aggregate functions
 */
public abstract class Sum implements AggregateFunction {

  @Override
  public PropertyValue aggregate(
    PropertyValue aggregate, PropertyValue increment) {

    return PropertyValueUtils.Numeric.add(aggregate, increment);
  }
}
