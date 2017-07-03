
package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Superclass of counting aggregate functions.
 */
public abstract class Count
  implements AggregateFunction, AggregateDefaultValue {

  @Override
  public PropertyValue aggregate(
    PropertyValue aggregate, PropertyValue increment) {
    return PropertyValueUtils.Numeric.add(aggregate, increment);
  }

  @Override
  public PropertyValue getDefaultValue() {
    return PropertyValue.create(0L);
  }
}
